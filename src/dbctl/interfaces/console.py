"""@brief dbctl 唯一操作者终端呈现边界 / Sole operator-console presentation boundary for dbctl."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import FrameType, ModuleType
from typing import Final, TextIO, assert_never

from rich.console import Console
from rich.text import Text

from dbctl.application.container_startup import ContainerLaunchError, ContainerProjectionError
from dbctl.application.errors import (
    ApplicationError,
    BootstrapExecutionError,
    ContainerEntrypointError,
    DatabaseAlreadyExistsError,
    DbctlConfigurationError,
    ExternalDiagnosticError,
    MigrationExecutionError,
    RetentionExecutionError,
    ShellExecutionError,
    redact_sensitive_text,
    safe_diagnostic_message_snapshot,
    safe_diagnostic_notes,
    safe_domain_error_message,
    safe_exception_category,
    safe_exception_cause,
    safe_exception_context,
    safe_exception_matches,
    safe_exception_suppresses_context,
    safe_exception_traceback,
)
from dbctl.application.migrate import MigrationRevision
from dbctl.application.progress import OperationName, ProgressSink, ProgressState, ProgressUpdate
from dbctl.application.provision import (
    BootstrapPlan,
    BootstrapResult,
    ExecutionTarget,
    SqlStatement,
    StageCondition,
)
from dbctl.application.prune_telemetry import (
    PruneApplied,
    PruneOutcome,
    PrunePreview,
    RetentionDisabled,
)
from dbctl.domain.errors import (
    DomainError,
    InvalidDatabaseModelError,
    InvalidNameError,
    InvalidRetentionPolicyError,
    InvalidRoleSetError,
)

_OPERATION_LABELS = {
    OperationName.CONFIGURATION: "配置",
    OperationName.BOOTSTRAP: "bootstrap",
    OperationName.CONTAINER: "container",
    OperationName.MIGRATION: "migration",
    OperationName.PRUNE_TELEMETRY: "prune-telemetry",
    OperationName.SHELL: "shell",
}
"""@brief 稳定操作名到中文终端标签的映射 / Stable operation-to-terminal labels."""

_STATE_PRESENTATION = {
    ProgressState.STARTED: ("→", "开始", "cyan"),
    ProgressState.SUCCEEDED: ("✓", "完成", "green"),
    ProgressState.SKIPPED: ("↷", "跳过", "yellow"),
    ProgressState.FAILED: ("✗", "失败", "bold red"),
}
"""@brief 进度状态的符号、文字和颜色 / Symbol, text, and color for each progress state."""

_DBCTL_PACKAGE_ROOT: Final[str] = os.path.realpath(str(Path(__file__).parents[1]))
"""@brief 唯一允许显示具体栈帧元数据的源码根 / Sole source root whose frame metadata may be shown."""

_MAX_FRAME_PATH_LENGTH: Final[int] = 1_024
"""@brief 进入路径处理前允许的最大 code filename / Maximum code filename before path processing."""

_MAX_FUNCTION_NAME_LENGTH: Final[int] = 256
"""@brief 可信函数名的最大字符数 / Maximum characters in a trusted function name."""

_MAX_EXCEPTION_CHAIN_DEPTH: Final[int] = 32
"""@brief 安全诊断允许遍历的最大异常链深度 / Maximum safe diagnostic chain depth."""

_SAFE_ERROR_LABELS: Final[tuple[tuple[type[BaseException], str], ...]] = (
    (ApplicationError, "dbctl.application.errors.ApplicationError"),
    (BootstrapExecutionError, "dbctl.application.errors.BootstrapExecutionError"),
    (ContainerEntrypointError, "dbctl.application.errors.ContainerEntrypointError"),
    (
        ContainerLaunchError,
        "dbctl.application.container_startup.ContainerLaunchError",
    ),
    (
        ContainerProjectionError,
        "dbctl.application.container_startup.ContainerProjectionError",
    ),
    (DatabaseAlreadyExistsError, "dbctl.application.errors.DatabaseAlreadyExistsError"),
    (DbctlConfigurationError, "dbctl.application.errors.DbctlConfigurationError"),
    (DomainError, "dbctl.domain.errors.DomainError"),
    (ExternalDiagnosticError, "dbctl.application.errors.ExternalDiagnosticError"),
    (InvalidDatabaseModelError, "dbctl.domain.errors.InvalidDatabaseModelError"),
    (InvalidNameError, "dbctl.domain.errors.InvalidNameError"),
    (InvalidRetentionPolicyError, "dbctl.domain.errors.InvalidRetentionPolicyError"),
    (InvalidRoleSetError, "dbctl.domain.errors.InvalidRoleSetError"),
    (MigrationExecutionError, "dbctl.application.errors.MigrationExecutionError"),
    (RetentionExecutionError, "dbctl.application.errors.RetentionExecutionError"),
    (ShellExecutionError, "dbctl.application.errors.ShellExecutionError"),
)
"""@brief 安全应用/领域错误的固定标签 / Fixed labels for safe application and domain errors."""


class OperatorConsole(ProgressSink):
    """@brief 同时服务 TTY 与耐久日志的操作者控制台 / Operator console for both TTYs and durable logs.

    @param stderr 过程、警告和诊断输出流 / Stream for progress, warnings, and diagnostics.
    @param quiet 是否抑制非必要过程消息 / Whether to suppress nonessential progress.
    @param no_color 是否明确禁用 ANSI 颜色 / Whether ANSI color is explicitly disabled.
    @note 主结果由 CLI 直接写 stdout；本类型只写 stderr，避免污染可管道化输出。
    / The CLI writes primary results directly to stdout; this type writes only stderr so pipeable
    output remains clean.
    """

    def __init__(self, stderr: TextIO, *, quiet: bool = False, no_color: bool = False) -> None:
        """@brief 创建一次命令使用的终端 adapter / Create a terminal adapter for one command.

        @param stderr 过程与诊断流 / Progress and diagnostic stream.
        @param quiet 是否抑制正常进度 / Whether normal progress is suppressed.
        @param no_color 是否明确禁用颜色 / Whether color is explicitly disabled.
        """

        color_disabled = (
            no_color or "NO_COLOR" in os.environ or os.environ.get("TERM", "").casefold() == "dumb"
        )
        self._console = Console(
            file=stderr,
            color_system=None if color_disabled else "auto",
            highlight=False,
            markup=False,
            no_color=color_disabled,
            soft_wrap=True,
        )
        self._quiet = quiet

    def announce(
        self,
        command: str,
        *,
        mode: str,
        config_path: Path | None,
        dbinit_path: Path | None,
    ) -> None:
        """@brief 在任何配置 I/O 前声明命令意图 / Announce command intent before configuration I/O.

        @param command 当前子命令 / Current subcommand.
        @param mode dry-run/apply、revision 或 shell role / Dry-run/apply mode, revision, or shell role.
        @param config_path 显式私密配置路径 / Explicit private-configuration path.
        @param dbinit_path 显式数据库声明路径 / Explicit database-declaration path.
        @return 无返回值 / No return value.
        """

        if self._quiet:
            return
        config = str(config_path) if config_path is not None else "config.jsonc（默认）"
        dbinit = (
            str(dbinit_path) if dbinit_path is not None else "dbinit.jsonc（缺失时读取内置资源）"
        )
        heading = Text("dbctl ", style="bold")
        heading.append(_one_line(command), style="bold cyan")
        self._print(heading)
        self._print(Text(f"  模式：{_one_line(mode)}", style="dim"))
        self._print(Text(f"  配置：{_one_line(config)}", style="dim"))
        self._print(Text(f"  声明：{_one_line(dbinit)}", style="dim"))

    def publish(self, update: ProgressUpdate) -> None:
        """@brief 将强类型进度渲染为稳定逐行记录 / Render typed progress as a stable line record.

        @param update 不含 secret 的应用进度 / Secret-free application progress.
        @return 无返回值 / No return value.
        """

        if self._quiet and update.state is not ProgressState.FAILED:
            return
        symbol, state_label, style = _STATE_PRESENTATION[update.state]
        operation = _OPERATION_LABELS[update.operation]
        position = (
            f" [{update.current}/{update.total}]"
            if update.current is not None and update.total is not None
            else ""
        )
        line = Text()
        line.append(f"{symbol} ", style=style)
        line.append(operation, style="bold")
        line.append(position, style="bold")
        line.append(f" · {state_label}：", style=style)
        line.append(_one_line(update.message))
        if update.detail is not None:
            line.append(" — ", style="dim")
            line.append(_one_line(update.detail), style="dim")
        self._print(line)

    def failure(self, command: str, error: BaseException, *, exit_code: int) -> None:
        """@brief 输出可行动摘要与已脱敏 traceback / Print an actionable summary and redacted traceback.

        @param command 失败的子命令 / Failed subcommand.
        @param error 带 traceback 的失败对象 / Failure carrying a traceback.
        @param exit_code CLI 将返回的状态 / Status the CLI will return.
        @return 无返回值 / No return value.
        @note traceback 不读取 locals、源码行、任意 Python notes 或外部异常正文；
        只有完全由安全异常类型组成的显式因果链才会显示。/ Tracebacks do not read locals,
        source lines, arbitrary Python notes, or external exception text; a cause chain is shown
        only when every member is an explicitly safe exception type.
        """

        approved_message = _approved_error_message(error)
        error_label = _safe_exception_label(error)
        reason = (
            approved_message
            if approved_message is not None
            else f"未预期的 {error_label}；原始消息已隐藏"
        )
        heading = Text("✗ ", style="bold red")
        heading.append(f"dbctl {_one_line(command)} 未完成", style="bold red")
        heading.append(f"（退出码 {exit_code}）", style="dim")
        self._print(heading)
        self._print(Text(f"  原因：{error_label.rpartition('.')[2]}: {reason}"))
        self._print(Text(f"  建议：{_failure_hint(error, command)}", style="yellow"))
        self._print(
            Text(
                "  Traceback（已脱敏；未读取 locals/源码行/外部异常正文）：",
                style="bold red",
            )
        )
        self._print(Text(_safe_traceback(error)))

    def cancelled(self, command: str, error: KeyboardInterrupt | None = None) -> None:
        """@brief 报告操作者通过中断取消命令 / Report cancellation through an operator interrupt.

        @param command 被取消的子命令 / Cancelled subcommand.
        @param error 可选中断对象，用于展示 dbctl 构造的安全影响注释。
        / Optional interrupt carrying dbctl-authored safe impact notes.
        @return 无返回值 / No return value.
        """

        self._print(
            Text(f"! dbctl {_one_line(command)} 已由操作者取消（退出码 130）", style="yellow")
        )
        if error is None:
            return
        for note in safe_diagnostic_notes(error):
            self._print(Text(f"  安全诊断：{note}", style="yellow"))

    def _print(self, value: Text) -> None:
        """@brief 尽力写 stderr 且不影响数据库操作 / Write stderr best-effort without affecting operations.

        @param value 已脱敏 Rich 文本 / Redacted Rich text.
        @return 无返回值 / No return value.
        @note 终端断开或 stderr 管道关闭只能损失呈现，不能改变提交/重试语义。
        / A detached terminal or closed stderr pipe may lose presentation but cannot alter commit or
        retry semantics.
        """

        try:
            self._console.print(value)
        except Exception:
            return


def render_bootstrap_plan(plan: BootstrapPlan) -> str:
    """@brief 渲染完全脱敏的 bootstrap dry-run / Render a fully redacted bootstrap dry-run.

    @param plan 自足有序计划 / Self-contained ordered plan.
    @return 不含参数原文的可管道化终端文本 / Pipeable terminal text containing no parameter values.
    """

    lines = [
        "-- dbctl bootstrap dry-run（不执行任何 SQL）",
        "-- 不修改 pg_hba.conf；不创建 PostgreSQL superuser。",
    ]
    for stage in plan.stages:
        target = "maintenance" if stage.target is ExecutionTarget.MAINTENANCE else "database"
        if stage.condition is StageCondition.DATABASE_ABSENT:
            target += "/conditional"
        for statement in stage.statements:
            lines.extend(
                (
                    f"-- [{target}] {statement.label}",
                    _render_redacted_statement(statement),
                )
            )
    return "\n".join(lines)


def render_bootstrap_result(result: BootstrapResult) -> str:
    """@brief 渲染 bootstrap 最终状态与下一步 / Render final bootstrap state and next action.

    @param result 不含 secret 的执行结果 / Secret-free execution result.
    @return 保持既有 stdout 契约的单行主结果 / Single-line result preserving the stdout contract.
    @note 目标、阶段和下一步已由 stderr 进度报告。
    / Target, stage, and next-action detail is reported on stderr.
    """

    database_status = "已创建" if result.database_created else "已存在"
    return (
        f"dbctl bootstrap 完成：目标数据库{database_status}；"
        f"执行了 {result.executed_statement_count} 条计划 SQL。"
    )


def render_migration_result(revision: MigrationRevision) -> str:
    """@brief 渲染 migration 完成状态 / Render completed migration state.

    @param revision 请求并完成的 Alembic 目标 / Requested and completed Alembic target.
    @return 保持既有 stdout 契约的单行主结果 / Single-line result preserving the stdout contract.
    @note 目标与身份已由 stderr 进度报告。
    / Target and identity are reported on stderr.
    """

    return f"dbctl migrate 完成：已升级至 {revision.value}。"


def render_prune_outcome(outcome: PruneOutcome) -> str:
    """@brief 穷尽渲染遥测清理判别联合 / Exhaustively render telemetry-prune outcomes.

    @param outcome 停用、预览或已执行结果 / Disabled, preview, or applied result.
    @return 不含 DSN、SQL 或驱动异常的中文摘要 / Chinese summary without DSN, SQL, or driver errors.
    """

    if isinstance(outcome, RetentionDisabled):
        return "dbctl prune-telemetry：observability.retention_days=0，清理已停用；未连接数据库。"
    if isinstance(outcome, PrunePreview):
        return (
            "dbctl prune-telemetry dry-run：不会连接数据库或执行删除；"
            f"保留 {outcome.policy.days} 天，删除早于 {outcome.cutoff.isoformat()} 的记录；"
            f"最多 {outcome.limits.max_batches} 批、"
            f"每批上限 {outcome.limits.batch_size} 条，"
            "语句超时由 --statement-timeout-ms 控制。"
        )
    if isinstance(outcome, PruneApplied):
        limit_note = "；已达到本次批次上限" if outcome.reached_batch_limit else ""
        remaining_note = "；仍有过期记录待下轮处理" if outcome.has_more else "；过期记录已清空"
        return (
            "dbctl prune-telemetry 完成："
            f"删除 {outcome.deleted_count} 条{remaining_note}；"
            f"cutoff 为 {outcome.cutoff.isoformat()}；"
            f"已提交 {outcome.batch_count}/{outcome.limits.max_batches} 个短事务{limit_note}。"
        )
    assert_never(outcome)


def _render_redacted_statement(statement: SqlStatement) -> str:
    """@brief 用固定标记替换每个 SQL 参数 / Replace every SQL parameter with a fixed marker.

    @param statement 参数化应用层 SQL / Parameterized application-layer SQL.
    @return 参数值不可见的 SQL / SQL with parameter values hidden.
    """

    pieces = statement.sql.split("%s")
    rendered = [pieces[0]]
    for index, _parameter in enumerate(statement.parameters):
        rendered.extend(("<redacted>", pieces[index + 1]))
    return "".join(rendered)


def _safe_traceback(error: BaseException) -> str:
    """@brief 仅遍历原生栈指针构造安全 traceback / Build a safe traceback from raw stack pointers only.

    @param error 当前失败 / Current failure.
    @return 保留安全因果链或隐藏未知消息的 traceback / Traceback with a safe chain or hidden unknown message.
    @note 不构造 ``TracebackException``；该类会读取异常正文、notes 与源码。
    / ``TracebackException`` is deliberately not constructed because it reads exception text,
    notes, and source lines.
    """

    errors, relationships = _displayed_exception_chain(error)
    rendered_parts: list[str] = []
    for index, current in enumerate(errors):
        rendered_parts.append(_format_traceback_exception(current))
        if index < len(relationships):
            rendered_parts.append(
                "\n上述安全诊断是后续异常的直接原因。\n\n"
                if relationships[index] == "cause"
                else "\n处理上述安全诊断时，又发生了后续异常。\n\n"
            )
    rendered = "".join(rendered_parts)
    notes = safe_diagnostic_notes(error)
    if notes:
        rendered += "".join(f"安全诊断：{note}\n" for note in notes)
    return rendered.rstrip("\n")


def _displayed_exception_chain(
    error: BaseException,
) -> tuple[tuple[BaseException, ...], tuple[str, ...]]:
    """@brief 按 traceback 顺序选取可展示异常链 / Select a display-safe exception chain in traceback order.

    @param error 顶层失败 / Top-level failure.
    @return 从最底层到顶层的异常及它们之间的 cause/context 关系。
    / Exceptions from root to top and the cause/context relationships between them.
    """

    errors: list[BaseException] = []
    relationships: list[str] = []
    current = error
    visited: set[int] = set()
    while len(errors) < _MAX_EXCEPTION_CHAIN_DEPTH:
        identity = id(current)
        if identity in visited or _approved_error_message(current) is None:
            return (error,), ()
        visited.add(identity)
        errors.append(current)
        cause = safe_exception_cause(current)
        context = safe_exception_context(current)
        if cause is not None:
            relationships.append("cause")
            current = cause
        elif context is not None and not safe_exception_suppresses_context(current):
            relationships.append("context")
            current = context
        else:
            errors.reverse()
            relationships.reverse()
            return tuple(errors), tuple(relationships)
    return (error,), ()


def _format_traceback_exception(error: BaseException) -> str:
    """@brief 只格式化受信任栈帧元数据与已批准消息 / Format trusted frame metadata and approved text only.

    @param error 待格式化异常 / Exception whose traceback is formatted.
    @return 不含源码行和任意底层消息的文本 / Text without source lines or arbitrary raw messages.
    """

    lines = ["Traceback (most recent call last):\n"]
    traceback_value = safe_exception_traceback(error)
    while traceback_value is not None:
        lines.append(_format_frame(traceback_value.tb_frame, traceback_value.tb_lineno))
        traceback_value = traceback_value.tb_next
    error_label = _safe_exception_label(error)
    message = _approved_error_message(error)
    if message is None:
        message = "<原始异常消息已隐藏；请依据异常类别和受信任栈帧诊断>"
    lines.append(f"{error_label}: {message}\n")
    return "".join(lines)


def _format_frame(frame: FrameType, line_number: int) -> str:
    """@brief 仅展示 dbctl package 内可验证的栈帧 / Show verifiable dbctl-package frames only.

    @param frame 原生 Python frame；只验证 globals 字典身份，不读取其值或 locals。
    / Native Python frame; only its globals-dictionary identity is verified, and locals are not read.
    @param line_number traceback 记录的正整数行号 / Positive line number recorded by the traceback.
    @return 可信 dbctl 位置或固定的外部帧占位 / Trusted dbctl location or a fixed external-frame placeholder.
    """

    location = _trusted_frame_location(frame)
    if location is None:
        return f"  <external frame hidden>, line {line_number}\n"
    safe_path, function_name = location
    return f'  File "dbctl/{safe_path}", line {line_number}, in {function_name}\n'


def _trusted_frame_location(frame: FrameType) -> tuple[str, str] | None:
    """@brief 以模块 namespace 身份验证 dbctl 栈帧 / Authenticate a dbctl frame by module namespace identity.

    @param frame 待验证原生 frame / Native frame to authenticate.
    @return package 相对路径与函数名，或固定隐藏信号 None。
    / Package-relative path and function name, or ``None`` to hide it.
    @note 仅伪造 ``co_filename`` 不足以通过：frame globals 必须与 ``sys.modules`` 中精确
    ``ModuleType`` 的 namespace 为同一对象，且 module ``__file__`` 必须匹配 code filename。
    / Forging ``co_filename`` alone is insufficient: frame globals must be identical to the
    namespace of an exact ``ModuleType`` in ``sys.modules``, and module ``__file__`` must match.
    """

    namespace = frame.f_globals
    module_name = dict.get(namespace, "__name__")
    if type(module_name) is not str or not (
        module_name == "dbctl" or module_name.startswith("dbctl.")
    ):
        return None
    module = sys.modules.get(module_name)
    if type(module) is not ModuleType:
        return None
    module_namespace = vars(module)
    if namespace is not module_namespace:
        return None

    code = frame.f_code
    filename = code.co_filename
    function_name = code.co_name
    module_file = dict.get(module_namespace, "__file__")
    if (
        type(module_file) is not str
        or not filename
        or len(filename) > _MAX_FRAME_PATH_LENGTH
        or not function_name
        or len(function_name) > _MAX_FUNCTION_NAME_LENGTH
    ):
        return None
    try:
        normalized = os.path.realpath(os.path.abspath(os.path.normpath(filename)))
        normalized_module_file = os.path.realpath(os.path.abspath(os.path.normpath(module_file)))
        if normalized != normalized_module_file:
            return None
        within_package = (
            os.path.commonpath((_DBCTL_PACKAGE_ROOT, normalized)) == _DBCTL_PACKAGE_ROOT
        )
        safe_path = os.path.relpath(normalized, _DBCTL_PACKAGE_ROOT).replace(os.sep, "/")
    except OSError, ValueError:
        return None
    if not within_package:
        return None
    path_is_safe = bool(safe_path) and all(
        character.isascii() and (character.isalnum() or character in "._-/")
        for character in safe_path
    )
    function_is_safe = all(
        character.isascii() and (character.isalnum() or character == "_")
        for character in function_name
    )
    return (safe_path, function_name) if path_is_safe and function_is_safe else None


def _safe_exception_label(error: BaseException) -> str:
    """@brief 从固定映射获取安全异常标签 / Get a safe exception label from fixed mappings.

    @param error 当前失败 / Current failure.
    @return 内部错误的固定类标签或外部基类分类 / Fixed internal label or external base category.
    """

    error_type = type(error)
    for known_type, label in _SAFE_ERROR_LABELS:
        if error_type is known_type:
            return label
    return safe_exception_category(error)


def _approved_error_message(error: BaseException) -> str | None:
    """@brief 一次验证并读取内部错误正文快照 / Validate and read an internal error snapshot once.

    @param error 候选应用或领域错误 / Candidate application or domain error.
    @return 已脱敏且来源获批的构造快照，其他情况为 None。
    / Redacted construction snapshot with approved provenance, otherwise ``None``.
    """

    error_type = type(error)
    known_type = any(error_type is candidate for candidate, _label in _SAFE_ERROR_LABELS)
    if not known_type:
        return None
    application_snapshot = safe_diagnostic_message_snapshot(error)
    if application_snapshot is not None:
        message, allows_external_origin = application_snapshot
        if allows_external_origin or _trusted_exception_origin(error):
            return message
        return None
    return safe_domain_error_message(error)


def _trusted_exception_origin(error: BaseException) -> bool:
    """@brief 验证异常最终 raise frame 来自真实 dbctl 模块 / Authenticate the final raise frame.

    @param error 待验证异常 / Exception to authenticate.
    @return traceback 最内层 frame 可信时为真 / True when the innermost traceback frame is trusted.
    """

    traceback_value = safe_exception_traceback(error)
    if traceback_value is None:
        return False
    while traceback_value.tb_next is not None:
        traceback_value = traceback_value.tb_next
    return _trusted_frame_location(traceback_value.tb_frame) is not None


def _failure_hint(error: BaseException, command: str) -> str:
    """@brief 将错误类别映射为可执行的下一步 / Map an error category to an actionable next step.

    @param error 当前失败 / Current failure.
    @param command 当前子命令 / Current subcommand.
    @return 不含 secret 的修复建议 / Secret-free recovery suggestion.
    """

    if _approved_error_message(error) is None:
        return (
            f"保留这份安全 traceback，并附上 `dbctl {_one_line(command)} --help` 输出提交问题报告。"
        )
    if safe_exception_matches(error, DbctlConfigurationError):
        return "检查上方 config/dbinit 路径、JSONC 字段及 config.jsonc 的 owner-only 权限。"
    if safe_exception_matches(error, BootstrapExecutionError):
        return "根据失败阶段核验 psql/sudo、PostgreSQL 可达性与管理权限；幂等阶段可修复后重试。"
    if safe_exception_matches(error, ContainerEntrypointError):
        return "先确认 dbctl 持久配置可投影；若投影已完成，再检查目标命令与容器用户执行权限。"
    if safe_exception_matches(error, MigrationExecutionError):
        return "先核验当前 Alembic revision、数据库锁和 migrator 权限，再决定是否重试。"
    if safe_exception_matches(error, RetentionExecutionError):
        return "先阅读“运维影响”确认已提交批次，再在维护窗口以相同边界重试。"
    if safe_exception_matches(error, ShellExecutionError):
        return "确认本机 psql 可执行、目标可达，并检查临时凭据文件的创建/清理权限。"
    if safe_exception_matches(error, DomainError):
        return "修正命令参数或 dbinit 中违反领域约束的值后重新运行。"
    return f"保留这份安全 traceback，并附上 `dbctl {_one_line(command)} --help` 输出提交问题报告。"


def _one_line(value: str) -> str:
    """@brief 将终端字段规范为脱敏单行文本 / Normalize a terminal field to one redacted line.

    @param value 待展示文本 / Candidate display text.
    @return 无控制序列的单行文本 / Single-line text without control sequences.
    """

    return redact_sensitive_text(value)


__all__ = [
    "OperatorConsole",
    "render_bootstrap_plan",
    "render_bootstrap_result",
    "render_migration_result",
    "render_prune_outcome",
]
