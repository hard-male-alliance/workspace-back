"""@brief 面向运维者的启发式 Dashboard CLI / Heuristic operator-facing Dashboard CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from typing import Never, TextIO

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from dashboard.application.dto import (
    DashboardOverview,
    DiagnosticEvent,
    EventReport,
    SystemHealthReport,
    TrendReport,
)
from dashboard.application.errors import DashboardError, DashboardReadStoreUnavailable
from dashboard.bootstrap import DashboardRuntime, build_runtime
from dashboard.domain.model import SignalKind

from .presenters import (
    JsonValue,
    event_report_payload,
    overview_payload,
    system_health_payload,
    trend_report_payload,
)
from .time import resolve_window

_VIEWS = (
    "overview",
    "services",
    "traffic",
    "latency",
    "errors",
    "saturation",
    "diagnostics",
    "frontend",
    "health",
)
"""@brief 用户可发现的只读视图 / Discoverable read-only views."""

_SIGNAL_VIEWS = {
    "traffic": SignalKind.TRAFFIC,
    "latency": SignalKind.LATENCY,
    "errors": SignalKind.ERRORS,
    "saturation": SignalKind.SATURATION,
}
"""@brief CLI 视图到领域黄金信号的映射 / Mapping from CLI views to domain golden signals."""


class DashboardArgumentParser(argparse.ArgumentParser):
    """@brief 将 argparse 错误保持在可注入 CLI 边界 / Keep argparse errors inside the injectable CLI boundary."""

    def error(self, message: str) -> Never:
        """@brief 将解析错误转换为普通异常 / Convert a parser error into a regular exception.

        @param message argparse 错误文本 / Argparse error text.
        @return 永不返回 / Never returns.
        """

        raise ValueError(message)


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 执行同步 console 入口 / Run the synchronous console entry point.

    @param argv 不含程序名的可选参数 / Optional arguments excluding the program name.
    @return 进程退出码 / Process exit code.
    """

    try:
        return asyncio.run(async_main(argv))
    except KeyboardInterrupt:
        return 130


async def async_main(
    argv: Sequence[str] | None = None,
    *,
    runtime: DashboardRuntime | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """@brief 执行可注入的异步 CLI / Run the injectable asynchronous CLI.

    @param argv 不含程序名的参数 / Arguments excluding the program name.
    @param runtime 可选外部运行时 / Optional externally owned runtime.
    @param stdout 可选输出流 / Optional output stream.
    @param stderr 可选错误流 / Optional error stream.
    @return 0 成功、1 存储不可用、2 输入或配置错误 / 0 success, 1 store unavailable, 2 input or configuration error.
    """

    output_stream = stdout or sys.stdout
    error_stream = stderr or sys.stderr
    parser = _build_parser()
    try:
        arguments = parser.parse_args(list(argv) if argv is not None else None)
    except SystemExit as error:
        return int(error.code) if isinstance(error.code, int) else 2
    except ValueError as error:
        print(f"dashboard: {error}", file=error_stream)
        return 2

    owns_runtime = runtime is None
    resolved_runtime: DashboardRuntime | None = None
    exit_code = 0
    try:
        resolved_runtime = runtime or build_runtime(config_path=arguments.config)
        principal = resolved_runtime.local_principal()
        window = resolve_window(
            since=arguments.since,
            start_at=arguments.start_at,
            end_at=arguments.end_at,
        )
        selected_view: str = arguments.view
        use_table = arguments.table or (not arguments.json and output_stream.isatty())
        demo_mode = resolved_runtime.settings.database.mode == "memory"
        if use_table and demo_mode:
            _console(output_stream).print(
                Panel(
                    "memory 适配器只展示空数据，不代表持久化运行态。",
                    title="DEMO MODE",
                    border_style="yellow",
                    padding=(0, 1),
                )
            )

        if selected_view == "health":
            report = await resolved_runtime.queries.system_health(principal, window=window)
            if use_table:
                _write_system_health_table(report, output_stream)
            else:
                _write_json(
                    _with_data_source(system_health_payload(report), demo_mode=demo_mode),
                    output_stream,
                )
        else:
            scope = resolved_runtime.workspace_scope(arguments.workspace)
            if selected_view in {"overview", "services"}:
                overview = await resolved_runtime.queries.overview(
                    principal,
                    scope,
                    window=window,
                    service=arguments.service,
                )
                if use_table:
                    _write_overview_table(
                        overview,
                        services_only=selected_view == "services",
                        stream=output_stream,
                    )
                else:
                    _write_json(
                        _with_data_source(
                            overview_payload(overview), demo_mode=demo_mode
                        ),
                        output_stream,
                    )
            elif selected_view in {"diagnostics", "frontend"}:
                events = await resolved_runtime.queries.recent_events(
                    principal,
                    scope,
                    window=window,
                    service=(
                        "frontend.browser"
                        if selected_view == "frontend"
                        else arguments.service
                    ),
                    limit=arguments.limit,
                )
                if use_table:
                    _write_event_table(events, output_stream)
                else:
                    _write_json(
                        _with_data_source(
                            event_report_payload(events), demo_mode=demo_mode
                        ),
                        output_stream,
                    )
            else:
                signal = _SIGNAL_VIEWS[selected_view]
                trends = await resolved_runtime.queries.trends(
                    principal,
                    scope,
                    signal,
                    window=window,
                    service=arguments.service,
                    bucket_seconds=arguments.bucket_seconds,
                )
                if use_table:
                    _write_trend_table(trends, output_stream)
                else:
                    _write_json(
                        _with_data_source(
                            trend_report_payload(trends), demo_mode=demo_mode
                        ),
                        output_stream,
                    )
    except DashboardReadStoreUnavailable:
        print("dashboard: 可观测性读模型当前不可用。", file=error_stream)
        exit_code = 1
    except (DashboardError, ValueError) as error:
        print(f"dashboard: {error}", file=error_stream)
        exit_code = 2
    except Exception:  # pragma: no cover - 进程级故障边界 / Process-level fault boundary.
        print("dashboard: 未预期错误；请查看受控日志。", file=error_stream)
        exit_code = 1
    finally:
        if owns_runtime and resolved_runtime is not None:
            try:
                await resolved_runtime.aclose()
            except Exception:  # pragma: no cover - 资源释放故障边界 / Cleanup fault boundary.
                print("dashboard: 关闭资源失败；请查看受控日志。", file=error_stream)
                exit_code = 1
    return exit_code


def _build_parser() -> DashboardArgumentParser:
    """@brief 构建零参数即有用的 CLI 解析器 / Build a CLI parser useful with zero arguments.

    @return argparse 解析器 / ``argparse`` parser.
    """

    parser = DashboardArgumentParser(
        prog="dashboard",
        description="查询工作区 SLO、黄金信号与诊断上下文；无参数时显示默认工作区总览。",
    )
    parser.add_argument("view", nargs="?", choices=_VIEWS, default="overview", help="要查看的运维视图。")
    parser.add_argument("--config", default="config.jsonc", help="根 JSONC 配置（默认：config.jsonc）。")
    parser.add_argument("--workspace", default=None, help="工作区；默认读取 workspace.default_workspace_id。")
    parser.add_argument("--service", default=None, help="仅查看一个稳定服务名。")
    parser.add_argument("--since", default=None, help="相对窗口，例如 30m、6h、7d。")
    parser.add_argument("--start-at", default=None, help="精确 RFC 3339 起点。")
    parser.add_argument("--end-at", default=None, help="精确 RFC 3339 终点。")
    parser.add_argument("--bucket-seconds", type=int, default=None, help="高级选项：趋势 SQL 桶宽。")
    parser.add_argument("--limit", type=int, default=100, help="diagnostics 最大事件数（默认：100）。")
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--json", action="store_true", help="强制 JSON 输出。")
    output.add_argument("--table", action="store_true", help="强制人类可读表格输出。")
    return parser


def _write_json(payload: object, stream: TextIO) -> None:
    """@brief 写出稳定 JSON / Write stable JSON.

    @param payload JSON 模型 / JSON model.
    @param stream 输出流 / Output stream.
    @return 无返回值 / No return value.
    """

    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), file=stream)


def _with_data_source(
    payload: dict[str, JsonValue],
    *,
    demo_mode: bool,
) -> dict[str, JsonValue]:
    """@brief 显式标记 demo 或持久化数据源 / Explicitly label demo or persistent data sources.

    @param payload 呈现对象 / Presentation object.
    @param demo_mode 是否使用空 memory demo adapter / Whether the empty memory demo adapter is active.
    @return 带数据源标签的新对象 / New object carrying a data-source label.
    """

    return {
        "data_source": "demo_empty_adapter" if demo_mode else "postgresql_observability",
        **payload,
    }


def _write_overview_table(
    report: DashboardOverview,
    *,
    services_only: bool,
    stream: TextIO,
) -> None:
    """@brief 渲染 Overview 或 services 表格 / Render an overview or services table.

    @param report Overview 报告 / Overview report.
    @param services_only 是否省略摘要卡片 / Whether to omit summary cards.
    @param stream 输出流 / Output stream.
    @return 无返回值 / No return value.
    """

    if not services_only:
        summary = Text()
        summary.append("状态  ", style="dim")
        summary.append(report.health.value.upper(), style=_health_style(report.health.value))
        summary.append("    请求  ", style="dim")
        summary.append(_number(report.request_count), style="bold")
        summary.append("    错误  ", style="dim")
        summary.append(_number(report.error_count), style="bold")
        summary.append("    燃烧率  ", style="dim")
        summary.append(f"{_optional(report.slo.burn_rate)}×", style="bold")
        freshness = (
            "无数据"
            if report.freshness.lag_seconds is None
            else f"{report.freshness.lag_seconds:.1f}s"
        )
        freshness_label = (
            "历史采集延迟"
            if report.freshness.mode.value == "historical"
            else "遥测新鲜度"
        )
        summary.append(f"\n{freshness_label}  ", style="dim")
        summary.append(freshness)
        summary.append("    空状态  ", style="dim")
        summary.append(str(report.no_data_reason or "-"))
        console = _console(stream)
        console.print(
            Panel(
                summary,
                title=Text(f"工作区 · {report.scope.workspace_id}"),
                border_style=_health_style(report.health.value),
                padding=(0, 1),
            )
        )
    rows = [
        (
            item.service,
            item.health.value,
            _number(item.request_count),
            _number(item.error_count),
            _percent(item.error_rate),
            _optional(item.latency_p95_ms),
            _percent(item.saturation_max),
        )
        for item in report.services
    ]
    _write_table(("服务", "状态", "请求", "错误", "错误率", "p95(ms)", "饱和度"), rows, stream)


def _write_trend_table(report: TrendReport, stream: TextIO) -> None:
    """@brief 按所选黄金信号渲染趋势 / Render trends for the selected golden signal.

    @param report 趋势报告 / Trend report.
    @param stream 输出流 / Output stream.
    @return 无返回值 / No return value.
    """

    headers: tuple[str, ...]
    rows: list[tuple[str, ...]]
    if report.signal is SignalKind.TRAFFIC:
        headers = ("时间", "服务", "请求")
        rows = [(_short_time(item.bucket_start), item.service, _number(item.request_count)) for item in report.points]
    elif report.signal is SignalKind.ERRORS:
        headers = ("时间", "服务", "错误", "错误率")
        rows = [
            (_short_time(item.bucket_start), item.service, _number(item.error_count), _percent(item.error_rate))
            for item in report.points
        ]
    elif report.signal is SignalKind.LATENCY:
        headers = ("时间", "服务", "p50(ms)", "p95(ms)", "p99(ms)")
        rows = [
            (
                _short_time(item.bucket_start),
                item.service,
                _optional(item.latency_p50_ms),
                _optional(item.latency_p95_ms),
                _optional(item.latency_p99_ms),
            )
            for item in report.points
        ]
    else:
        headers = ("时间", "服务", "平均饱和度", "峰值饱和度")
        rows = [
            (
                _short_time(item.bucket_start),
                item.service,
                _percent(item.saturation_mean),
                _percent(item.saturation_max),
            )
            for item in report.points
        ]
    _write_table(headers, rows, stream)


def _write_event_table(report: EventReport, stream: TextIO) -> None:
    """@brief 渲染诊断事件表 / Render a diagnostic-event table.

    @param report 诊断事件报告 / Diagnostic-event report.
    @param stream 输出流 / Output stream.
    @return 无返回值 / No return value.
    """

    rows = [
        (
            _short_time(item.occurred_at),
            item.service,
            item.kind,
            item.severity_text or "-",
            _event_value(item),
            item.name,
            item.trace_id or "-",
        )
        for item in report.events
    ]
    _write_table(
        ("发生时间", "服务", "类型", "严重度", "值/时长", "名称", "Trace"),
        rows,
        stream,
    )


def _write_system_health_table(report: SystemHealthReport, stream: TextIO) -> None:
    """@brief 渲染 operator-only 系统健康卡片 / Render an operator-only system-health card.

    @param report 系统健康报告 / System-health report.
    @param stream 输出流 / Output stream.
    @return 无返回值 / No return value.
    """

    body = Text()
    body.append("状态  ", style="dim")
    body.append(report.health.value.upper(), style=_health_style(report.health.value))
    body.append("    严重度  ", style="dim")
    body.append(report.severity_text or "-")
    rows = (
        ("accepted", report.accepted_count),
        ("pipeline dropped", report.dropped_count),
        ("write failures", report.write_failure_count),
        ("output dropped", report.output_dropped_count),
    )
    for label, value in rows:
        body.append(f"\n{label}  ", style="dim")
        body.append("-" if value is None else f"{value:,}", style="bold")
    _console(stream).print(
        Panel(
            body,
            title="系统 · Observability Pipeline",
            border_style=_health_style(report.health.value),
            padding=(0, 1),
        )
    )


def _event_value(event: DiagnosticEvent) -> str:
    """@brief 格式化 metric 或 span 的受控数值 / Format a controlled metric or span value.

    @param event 诊断事件 / Diagnostic event.
    @return 值、时长或占位符 / Value, duration, or placeholder.
    """

    if event.value is not None:
        return f"{event.value:.4g} {event.unit or ''}".rstrip()
    if event.duration_ms is not None:
        return f"{event.duration_ms:.2f} ms"
    return "-"


def _write_table(headers: tuple[str, ...], rows: Sequence[tuple[str, ...]], stream: TextIO) -> None:
    """@brief 以 Rich 正确渲染中文宽度与空态 / Use Rich to render CJK widths and empty states correctly.

    @param headers 列标题 / Column headers.
    @param rows 已格式化行 / Preformatted rows.
    @param stream 输出流 / Output stream.
    @return 无返回值 / No return value.
    """

    table = Table(
        box=box.SIMPLE_HEAVY,
        header_style="bold cyan",
        show_edge=False,
        pad_edge=False,
        highlight=False,
    )
    for header in headers:
        table.add_column(header, overflow="fold")
    for row in rows:
        table.add_row(*(Text(value) for value in row))
    console = _console(stream)
    console.print(table)
    if not rows:
        console.print(Panel("当前窗口无数据", border_style="dim", padding=(0, 1)))


def _console(stream: TextIO) -> Console:
    """@brief 为目标流创建确定性的 Rich Console / Create a deterministic Rich console for a target stream.

    @param stream 输出流 / Output stream.
    @return Rich Console / Rich ``Console``.
    """

    terminal = stream.isatty()
    return Console(
        file=stream,
        force_terminal=terminal,
        no_color=not terminal,
        soft_wrap=False,
    )


def _health_style(value: str) -> str:
    """@brief 将健康状态映射为一致颜色 / Map health status to a consistent color.

    @param value 健康枚举值 / Health enum value.
    @return Rich style / Rich style.
    """

    return {
        "healthy": "green",
        "degraded": "yellow",
        "critical": "bold red",
        "no_data": "dim",
    }.get(value, "white")


def _short_time(value: object) -> str:
    """@brief 紧凑格式化时间 / Compactly format a timestamp.

    @param value datetime-like 值 / Datetime-like value.
    @return 紧凑 UTC 文本 / Compact UTC text.
    """

    return value.strftime("%m-%d %H:%M:%S") if hasattr(value, "strftime") else str(value)


def _number(value: float) -> str:
    """@brief 紧凑格式化计数 / Compactly format a count.

    @param value 数值 / Numeric value.
    @return 文本 / Text.
    """

    return f"{value:,.0f}" if value.is_integer() else f"{value:,.2f}"


def _optional(value: float | None) -> str:
    """@brief 格式化可空浮点值 / Format a nullable floating-point value.

    @param value 可空数值 / Nullable numeric value.
    @return 文本 / Text.
    """

    return "-" if value is None else f"{value:.2f}"


def _percent(value: float | None) -> str:
    """@brief 格式化可空比例 / Format a nullable ratio.

    @param value 可空比例 / Nullable ratio.
    @return 百分比文本 / Percentage text.
    """

    return "-" if value is None else f"{value * 100:.2f}%"


if __name__ == "__main__":  # pragma: no cover - 标准入口 / Standard entry point.
    raise SystemExit(main())


__all__ = ["async_main", "main"]
