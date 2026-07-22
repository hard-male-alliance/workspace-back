"""@brief dbctl 唯一操作者终端呈现边界 / Sole operator-console presentation boundary for dbctl."""

from __future__ import annotations

import os
import traceback
from pathlib import Path
from typing import TextIO, assert_never

from rich.console import Console
from rich.text import Text

from dbctl.application.errors import (
    ApplicationError,
    BootstrapExecutionError,
    DbctlConfigurationError,
    ExternalDiagnosticError,
    MigrationExecutionError,
    RetentionExecutionError,
    ShellExecutionError,
    redact_sensitive_text,
    safe_diagnostic_notes,
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
from dbctl.domain.database import LoginDatabase
from dbctl.domain.errors import DomainError

_OPERATION_LABELS = {
    OperationName.CONFIGURATION: "配置",
    OperationName.BOOTSTRAP: "bootstrap",
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
            no_color
            or "NO_COLOR" in os.environ
            or os.environ.get("TERM", "").casefold() == "dumb"
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
        @note traceback 不捕获 locals；只有完全由安全异常类型组成的显式因果链才会显示。
        / Tracebacks never capture locals, and a cause chain is shown only when every member is an
        explicitly safe exception type.
        """

        safe_error = isinstance(error, (ApplicationError, DomainError))
        reason = (
            redact_sensitive_text(str(error))
            if safe_error
            else f"未预期的 {type(error).__module__}.{type(error).__qualname__}；原始消息已隐藏"
        )
        heading = Text("✗ ", style="bold red")
        heading.append(f"dbctl {_one_line(command)} 未完成", style="bold red")
        heading.append(f"（退出码 {exit_code}）", style="dim")
        self._print(heading)
        self._print(Text(f"  原因：{type(error).__name__}: {reason}"))
        self._print(Text(f"  建议：{_failure_hint(error, command)}", style="yellow"))
        self._print(Text("  Traceback（已脱敏；未捕获 locals）：", style="bold red"))
        self._print(Text(_safe_traceback(error)))

    def cancelled(self, command: str) -> None:
        """@brief 报告操作者通过中断取消命令 / Report cancellation through an operator interrupt.

        @param command 被取消的子命令 / Cancelled subcommand.
        @return 无返回值 / No return value.
        """

        self._print(
            Text(f"! dbctl {_one_line(command)} 已由操作者取消（退出码 130）", style="yellow")
        )

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

    statement_count = sum(len(stage.statements) for stage in plan.stages)
    lines = [
        "-- dbctl bootstrap dry-run（不执行任何 SQL）",
        (
            "-- 目标："
            f"{plan.database_target.host}:{plan.database_target.port}/{plan.database.value}；"
            f"{len(plan.stages)} 个阶段，{statement_count} 条计划 SQL。"
        ),
        "-- 本地配置初始化可能已完成；本命令未连接 PostgreSQL。",
        "-- 不修改 pg_hba.conf；不创建 PostgreSQL superuser。",
    ]
    for stage_number, stage in enumerate(plan.stages, start=1):
        target = "maintenance" if stage.target is ExecutionTarget.MAINTENANCE else "database"
        if stage.condition is StageCondition.DATABASE_ABSENT:
            target += "/conditional"
        lines.append(
            f"-- === 阶段 {stage_number}/{len(plan.stages)}：{stage.label} "
            f"[{target}; {stage.transaction_mode.value}] ==="
        )
        for statement in stage.statements:
            lines.extend(
                (
                    f"-- [{target}] {statement.label}",
                    _render_redacted_statement(statement),
                )
            )
    lines.extend(
        (
            "-- dry-run 完成：数据库状态未改变。",
            "-- 下一步：审阅计划后运行 `dbctl bootstrap`，再运行 `dbctl migrate --revision head`。",
        )
    )
    return "\n".join(lines)


def render_bootstrap_result(result: BootstrapResult, plan: BootstrapPlan) -> str:
    """@brief 渲染 bootstrap 最终状态与下一步 / Render final bootstrap state and next action.

    @param result 不含 secret 的执行结果 / Secret-free execution result.
    @param plan 本轮执行的不可变计划 / Immutable plan executed in this run.
    @return 保持既有 stdout 契约的单行主结果 / Single-line result preserving the stdout contract.
    @note 目标、阶段和下一步已由 stderr 进度报告；保留 ``plan`` 参数兼容既有调用。
    / Target, stage, and next-action detail is reported on stderr; ``plan`` remains for call compatibility.
    """

    del plan
    database_status = "已创建" if result.database_created else "已存在"
    return (
        f"dbctl bootstrap 完成：目标数据库{database_status}；"
        f"执行了 {result.executed_statement_count} 条计划 SQL。"
    )


def render_migration_result(revision: MigrationRevision, login: LoginDatabase) -> str:
    """@brief 渲染 migration 完成状态 / Render completed migration state.

    @param revision 请求并完成的 Alembic 目标 / Requested and completed Alembic target.
    @param login 本轮使用的 migrator 登录 / Migrator login used by this run.
    @return 保持既有 stdout 契约的单行主结果 / Single-line result preserving the stdout contract.
    @note 目标与身份已由 stderr 进度报告；保留 ``login`` 参数兼容既有调用。
    / Target and identity are reported on stderr; ``login`` remains for call compatibility.
    """

    del login
    return f"dbctl migrate 完成：已升级至 {revision.value}。"


def render_prune_outcome(outcome: PruneOutcome) -> str:
    """@brief 穷尽渲染遥测清理判别联合 / Exhaustively render telemetry-prune outcomes.

    @param outcome 停用、预览或已执行结果 / Disabled, preview, or applied result.
    @return 不含 DSN、SQL 或驱动异常的中文摘要 / Chinese summary without DSN, SQL, or driver errors.
    """

    if isinstance(outcome, RetentionDisabled):
        return (
            "dbctl prune-telemetry：observability.retention_days=0，"
            "清理已停用；未连接数据库。"
        )
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
    """@brief 构造不捕获 locals 的安全 traceback / Build a safe traceback without captured locals.

    @param error 当前失败 / Current failure.
    @return 保留安全因果链或隐藏未知消息的 traceback / Traceback with a safe chain or hidden unknown message.
    """

    if isinstance(error, (ApplicationError, DomainError)):
        trace = traceback.TracebackException.from_exception(
            error,
            capture_locals=False,
            compact=True,
        )
        _discard_untrusted_traceback_notes(trace)
        rendered = "".join(trace.format(chain=_chain_is_safe(error)))
    else:
        stack = "".join(traceback.format_tb(error.__traceback__))
        error_type = f"{type(error).__module__}.{type(error).__qualname__}"
        rendered = (
            "Traceback (most recent call last):\n"
            f"{stack}{error_type}: <原始异常消息已隐藏；请依据异常类型和栈帧诊断>\n"
        )
    notes = safe_diagnostic_notes(error)
    if notes:
        rendered += "".join(f"安全诊断：{note}\n" for note in notes)
    return "\n".join(_redact_traceback_line(line) for line in rendered.splitlines())


def _redact_traceback_line(line: str) -> str:
    """@brief 脱敏 traceback 行且保留结构缩进 / Redact a traceback line while preserving indentation.

    @param line 单个 traceback 行 / One traceback line.
    @return 无控制符但保留前导空白的行 / Control-free line retaining leading whitespace.
    """

    indentation_length = len(line) - len(line.lstrip(" \t"))
    indentation = line[:indentation_length]
    content = redact_sensitive_text(line[indentation_length:])
    return indentation + content


def _discard_untrusted_traceback_notes(trace: traceback.TracebackException) -> None:
    """@brief 从格式化对象递归移除任意 Python notes / Recursively remove arbitrary Python notes.

    @param trace 不捕获 locals 的 traceback 格式化对象 / Traceback formatter without locals.
    @return 无返回值 / No return value.
    @note 只有 ``safe_diagnostic_notes`` 返回的 dbctl 专用注释会在随后单独渲染。
    / Only dbctl-specific notes returned by ``safe_diagnostic_notes`` are rendered separately.
    """

    trace.__notes__ = None
    if trace.__cause__ is not None:
        _discard_untrusted_traceback_notes(trace.__cause__)
    if trace.__context__ is not None:
        _discard_untrusted_traceback_notes(trace.__context__)
    if trace.exceptions is not None:
        for nested in trace.exceptions:
            _discard_untrusted_traceback_notes(nested)


def _chain_is_safe(error: BaseException) -> bool:
    """@brief 只允许显式安全异常类型进入显示链 / Allow only explicitly safe types in the displayed chain.

    @param error traceback 顶层异常 / Top-level traceback exception.
    @return 整条 cause/context 链均安全时为真 / True when every cause/context member is safe.
    """

    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if not isinstance(current, (ApplicationError, DomainError, ExternalDiagnosticError)):
            return False
        if current.__cause__ is not None:
            current = current.__cause__
        elif current.__context__ is not None and not current.__suppress_context__:
            current = current.__context__
        else:
            current = None
    return current is None


def _failure_hint(error: BaseException, command: str) -> str:
    """@brief 将错误类别映射为可执行的下一步 / Map an error category to an actionable next step.

    @param error 当前失败 / Current failure.
    @param command 当前子命令 / Current subcommand.
    @return 不含 secret 的修复建议 / Secret-free recovery suggestion.
    """

    if isinstance(error, DbctlConfigurationError):
        return "检查上方 config/dbinit 路径、JSONC 字段及 config.jsonc 的 owner-only 权限。"
    if isinstance(error, BootstrapExecutionError):
        return "根据失败阶段核验 psql/sudo、PostgreSQL 可达性与管理权限；幂等阶段可修复后重试。"
    if isinstance(error, MigrationExecutionError):
        return "先核验当前 Alembic revision、数据库锁和 migrator 权限，再决定是否重试。"
    if isinstance(error, RetentionExecutionError):
        return "先阅读“运维影响”确认已提交批次，再在维护窗口以相同边界重试。"
    if isinstance(error, ShellExecutionError):
        return "确认本机 psql 可执行、目标可达，并检查临时凭据文件的创建/清理权限。"
    if isinstance(error, DomainError):
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
