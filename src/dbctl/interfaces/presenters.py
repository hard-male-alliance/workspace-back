"""@brief dbctl 终端安全 presenter / Secret-safe terminal presenters for dbctl."""

from typing import assert_never

from dbctl.application.provision import (
    BootstrapPlan,
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


def render_bootstrap_plan(plan: BootstrapPlan) -> str:
    """@brief 渲染完全脱敏的 bootstrap dry-run / Render a fully redacted bootstrap dry-run.

    @param plan 自足有序计划 / Self-contained ordered plan.
    @return 不含参数原文的终端文本 / Terminal text containing no parameter values.
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
            f"每批上限 {outcome.limits.batch_size} 条，语句超时由 --statement-timeout-ms 控制。"
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


def render_shell_policy(login: LoginDatabase) -> str:
    """@brief 渲染不含凭证的 shell 认证说明 / Render a credential-free shell authentication notice.

    @param login 即将使用的强类型登录 / Purpose-typed login about to be used.
    @return 安全 stderr 文本 / Safe stderr text.
    """

    return f"dbctl shell：自动使用 config.jsonc 中的 {login.role.value} role 与密码。"


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


__all__ = ["render_bootstrap_plan", "render_prune_outcome", "render_shell_policy"]
