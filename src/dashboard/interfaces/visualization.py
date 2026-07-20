"""@brief Dashboard 趋势可视化适配器 / Dashboard trend-visualization adapters."""

from __future__ import annotations

from collections import defaultdict
from importlib import import_module
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from dashboard.application.dto import TrendPoint, TrendReport
from dashboard.application.errors import DashboardDependencyError
from dashboard.domain.model import SignalKind

_COLORS = ("#60A5FA", "#34D399", "#FBBF24", "#F87171", "#A78BFA", "#22D3EE")
"""@brief 暗色主题的可访问趋势色板 / Accessible dark-theme trend palette."""


def create_matplotlib_figure(report: TrendReport | None = None) -> Any:
    """@brief 延迟创建嵌入式 Matplotlib Figure / Lazily create an embeddable Matplotlib figure.

    @param report 可选初始趋势 / Optional initial trend.
    @return Matplotlib Figure / Matplotlib ``Figure``.
    """

    try:
        figure_module = import_module("matplotlib.figure")
    except ModuleNotFoundError as error:
        raise DashboardDependencyError(
            "Dashboard GUI 需要可选依赖 matplotlib；CLI 与 API 不需要安装它。"
        ) from error
    figure = figure_module.Figure(figsize=(10, 5), facecolor="#0B1220")
    if report is None:
        _draw_empty(figure, "选择视图并刷新以查看趋势")
    else:
        draw_matplotlib_trend(figure, report)
    return figure


def draw_matplotlib_trend(figure: Any, report: TrendReport) -> None:
    """@brief 在已有 Figure 上绘制趋势 / Draw a trend on an existing Figure.

    @param figure Matplotlib Figure / Matplotlib ``Figure``.
    @param report 趋势应用 DTO / Trend application DTO.
    @return 无返回值 / No return value.
    """

    figure.clear()
    axis = figure.subplots()
    _style_axis(axis)
    grouped = _series(report)
    for index, (service, points) in enumerate(sorted(grouped.items())):
        x_values = [point.bucket_start for point in points]
        y_values = [_signal_value(point, report.signal) for point in points]
        pairs = [(x, y) for x, y in zip(x_values, y_values, strict=True) if y is not None]
        if not pairs:
            continue
        axis.plot(
            [item[0] for item in pairs],
            [item[1] for item in pairs],
            color=_COLORS[index % len(_COLORS)],
            linewidth=2.0,
            marker="o" if len(pairs) < 20 else None,
            markersize=3,
            label=service,
        )
    axis.set_title(_signal_title(report.signal), color="#F8FAFC", loc="left", pad=14, fontsize=13)
    axis.set_ylabel(_signal_unit(report.signal), color="#CBD5E1")
    if grouped:
        legend = axis.legend(frameon=False, ncols=min(3, len(grouped)))
        for label in legend.get_texts():
            label.set_color("#CBD5E1")
    else:
        axis.text(
            0.5,
            0.5,
            f"当前窗口无数据 · {report.no_data_reason or 'unknown'}",
            transform=axis.transAxes,
            ha="center",
            va="center",
            color="#94A3B8",
        )
    figure.autofmt_xdate()
    figure.tight_layout(pad=1.5)


def render_plotly_html(report: TrendReport) -> str:
    """@brief 生成可分享的独立 Plotly HTML 报告 / Generate a shareable standalone Plotly HTML report.

    @param report 趋势应用 DTO / Trend application DTO.
    @return 自包含 HTML / Self-contained HTML.
    """

    try:
        graph_objects = import_module("plotly.graph_objects")
    except ModuleNotFoundError as error:
        raise DashboardDependencyError(
            "交互式 Dashboard 报告需要可选依赖 plotly。"
        ) from error
    figure = graph_objects.Figure()
    for index, (service, points) in enumerate(sorted(_series(report).items())):
        values = [_signal_value(point, report.signal) for point in points]
        figure.add_trace(
            graph_objects.Scatter(
                x=[point.bucket_start for point in points],
                y=values,
                mode="lines+markers",
                name=service,
                line={"color": _COLORS[index % len(_COLORS)], "width": 2},
                hovertemplate="%{x}<br>%{y:.3f}<extra>%{fullData.name}</extra>",
            )
        )
    figure.update_layout(
        template="plotly_dark",
        title={"text": _signal_title(report.signal), "x": 0.02},
        xaxis_title="UTC 时间",
        yaxis_title=_signal_unit(report.signal),
        paper_bgcolor="#0B1220",
        plot_bgcolor="#111827",
        font={"family": "Inter, Noto Sans CJK SC, sans-serif", "color": "#E2E8F0"},
        hovermode="x unified",
        margin={"l": 60, "r": 30, "t": 70, "b": 60},
    )
    return str(
        figure.to_html(
            full_html=True,
            include_plotlyjs=True,
            config={"displaylogo": False, "responsive": True},
        )
    )


def write_plotly_report(report: TrendReport, path: str | Path) -> Path:
    """@brief 原子性写出 Plotly HTML 报告 / Atomically write a Plotly HTML report.

    @param report 趋势应用 DTO / Trend application DTO.
    @param path 用户选择的目标文件 / User-selected target file.
    @return 最终路径 / Final path.
    """

    target = Path(path)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(render_plotly_html(report))
        temporary.replace(target)
    except BaseException:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise
    return target


def _series(report: TrendReport) -> dict[str, list[TrendPoint]]:
    """@brief 按服务分组并稳定排序趋势点 / Group and stably sort trend points by service.

    @param report 趋势报告 / Trend report.
    @return 服务到点的映射 / Service-to-points mapping.
    """

    grouped: defaultdict[str, list[TrendPoint]] = defaultdict(list)
    for point in report.points:
        grouped[point.service].append(point)
    return {
        service: sorted(points, key=lambda point: point.bucket_start)
        for service, points in grouped.items()
    }


def _signal_value(point: TrendPoint, signal: SignalKind) -> float | None:
    """@brief 选择当前视图的 y 值 / Select the y value for the active view.

    @param point 趋势点 / Trend point.
    @param signal 黄金信号 / Golden signal.
    @return 可空 y 值 / Nullable y value.
    """

    if signal is SignalKind.TRAFFIC:
        return point.request_count
    if signal is SignalKind.ERRORS:
        return None if point.error_rate is None else point.error_rate * 100.0
    if signal is SignalKind.LATENCY:
        return point.latency_p95_ms
    return None if point.saturation_max is None else point.saturation_max * 100.0


def _signal_title(signal: SignalKind) -> str:
    """@brief 返回图表标题 / Return the chart title.

    @param signal 黄金信号 / Golden signal.
    @return 中文标题 / Chinese title.
    """

    return {
        SignalKind.TRAFFIC: "流量 · 每时间桶请求数",
        SignalKind.ERRORS: "错误 · 错误率",
        SignalKind.LATENCY: "延迟 · p95",
        SignalKind.SATURATION: "饱和度 · 峰值",
    }[signal]


def _signal_unit(signal: SignalKind) -> str:
    """@brief 返回图表单位 / Return the chart unit.

    @param signal 黄金信号 / Golden signal.
    @return 单位文本 / Unit text.
    """

    if signal is SignalKind.TRAFFIC:
        return "请求 / bucket"
    if signal is SignalKind.LATENCY:
        return "毫秒 (ms)"
    return "百分比 (%)"


def _style_axis(axis: Any) -> None:
    """@brief 应用一致的暗色视觉系统 / Apply the consistent dark visual system.

    @param axis Matplotlib Axes / Matplotlib ``Axes``.
    @return 无返回值 / No return value.
    """

    axis.set_facecolor("#111827")
    axis.grid(True, color="#334155", alpha=0.45, linewidth=0.7)
    axis.tick_params(colors="#94A3B8")
    for spine in axis.spines.values():
        spine.set_color("#334155")


def _draw_empty(figure: Any, message: str) -> None:
    """@brief 绘制空状态 / Draw an empty state.

    @param figure Matplotlib Figure / Matplotlib ``Figure``.
    @param message 空状态文本 / Empty-state text.
    @return 无返回值 / No return value.
    """

    axis = figure.subplots()
    _style_axis(axis)
    axis.text(0.5, 0.5, message, transform=axis.transAxes, ha="center", va="center", color="#94A3B8")
    axis.set_xticks([])
    axis.set_yticks([])
    figure.tight_layout()


__all__ = [
    "create_matplotlib_figure",
    "draw_matplotlib_trend",
    "render_plotly_html",
    "write_plotly_report",
]
