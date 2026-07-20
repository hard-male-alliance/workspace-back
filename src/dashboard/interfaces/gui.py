"""@brief 非阻塞桌面 Dashboard / Non-blocking desktop Dashboard."""

from __future__ import annotations

import argparse
import asyncio
import sys
import threading
from collections.abc import Callable, Coroutine, Sequence
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol, TypeVar

from dashboard.application.dto import DashboardOverview, TrendReport
from dashboard.application.errors import (
    DashboardDependencyError,
    DashboardError,
    DashboardQueryError,
)
from dashboard.bootstrap import DashboardRuntime, build_runtime
from dashboard.domain.model import FreshnessMode, SignalKind, TimeWindow

from .visualization import create_matplotlib_figure, draw_matplotlib_trend, write_plotly_report

T = TypeVar("T")
"""@brief 后台协程结果类型 / Background-coroutine result type."""

_WINDOWS = {
    "15 分钟": timedelta(minutes=15),
    "1 小时": timedelta(hours=1),
    "6 小时": timedelta(hours=6),
    "24 小时": timedelta(days=1),
    "7 天": timedelta(days=7),
}
"""@brief GUI 的启发式时间窗口 / Heuristic GUI time windows."""

_GUI_STYLE = """
QMainWindow, QWidget { background: #0B1220; color: #E2E8F0; }
QLabel#title { font-size: 24px; font-weight: 700; color: #F8FAFC; }
QLabel#subtitle { color: #94A3B8; }
QFrame#card { background: #111827; border: 1px solid #1E293B; border-radius: 10px; }
QLabel#cardTitle { color: #94A3B8; font-size: 12px; }
QLabel#cardValue { color: #F8FAFC; font-size: 20px; font-weight: 650; }
QLineEdit, QComboBox { background: #111827; border: 1px solid #334155; border-radius: 6px; padding: 7px; }
QPushButton { background: #2563EB; border: 0; border-radius: 6px; padding: 8px 14px; font-weight: 600; }
QPushButton:hover { background: #3B82F6; }
QPushButton:disabled { background: #334155; color: #64748B; }
QTableWidget { background: #111827; border: 1px solid #1E293B; gridline-color: #1E293B; }
QHeaderView::section { background: #172033; color: #CBD5E1; border: 0; padding: 7px; }
"""
"""@brief GUI 一致暗色视觉系统 / Consistent dark GUI visual system."""


@dataclass(frozen=True, slots=True)
class GuiSnapshot:
    """@brief 一次原子 GUI 刷新结果 / One atomic GUI refresh result.

    @param overview Overview DTO / Overview DTO.
    @param trend 当前黄金信号趋势 / Active golden-signal trend.
    """

    overview: DashboardOverview
    trend: TrendReport


@dataclass(frozen=True, slots=True)
class _GuiModules:
    """@brief 延迟导入的 GUI 模块 / Lazily imported GUI modules."""

    core: Any
    widgets: Any
    backend: Any


class AsyncClosable(Protocol):
    """@brief 可在 worker loop 关闭的运行时协议 / Runtime protocol closable on the worker loop."""

    async def aclose(self) -> None:
        """@brief 异步关闭资源 / Close resources asynchronously.

        @return 无返回值 / No return value.
        """

        ...


class AsyncQueryWorker:
    """@brief 在持久后台事件循环运行数据库查询 / Run database queries on a persistent background event loop.

    @note Qt UI 线程从不调用 ``asyncio.run`` 或等待数据库 future，因此拖动、重绘和关闭窗口
    保持响应。/ The Qt UI thread never calls ``asyncio.run`` or waits for database futures, keeping
    window movement, repainting, and shutdown responsive.
    """

    def __init__(self) -> None:
        """@brief 启动持久后台事件循环 / Start the persistent background event loop.

        @return 新 worker / New worker.
        """

        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._closed = False
        self._futures: set[Future[Any]] = set()
        self._futures_lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="dashboard-query-worker",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise DashboardQueryError("Dashboard GUI 查询 worker 启动超时。")

    def submit(
        self,
        coroutine: Coroutine[Any, Any, T],
        on_success: Callable[[T], None],
        on_error: Callable[[Exception], None],
    ) -> None:
        """@brief 提交协程并通过线程安全回调交付结果 / Submit a coroutine and deliver it through thread-safe callbacks.

        @param coroutine 待运行协程 / Coroutine to run.
        @param on_success 成功回调 / Success callback.
        @param on_error 失败回调 / Failure callback.
        @return 无返回值 / No return value.
        """

        if self._closed:
            coroutine.close()
            on_error(DashboardQueryError("Dashboard GUI 查询 worker 已关闭。"))
            return
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        with self._futures_lock:
            self._futures.add(future)

        def completed(done: Future[T]) -> None:
            """@brief 转发一个后台 Future / Forward one background future.

            @param done 已完成 future / Completed future.
            @return 无返回值 / No return value.
            """

            with self._futures_lock:
                self._futures.discard(done)
            try:
                on_success(done.result())
            except Exception as error:
                on_error(error)

        future.add_done_callback(completed)

    def run_until_complete(self, coroutine: Coroutine[Any, Any, T], *, timeout: float = 5.0) -> T:
        """@brief 在 worker loop 有界执行生命周期协程 / Boundedly run a lifecycle coroutine on the worker loop.

        @param coroutine 生命周期协程 / Lifecycle coroutine.
        @param timeout 最大等待秒数 / Maximum wait in seconds.
        @return 协程结果 / Coroutine result.
        """

        if self._closed:
            coroutine.close()
            raise DashboardQueryError("Dashboard GUI 查询 worker 已关闭。")
        if threading.current_thread() is self._thread:
            coroutine.close()
            raise DashboardQueryError("不能从 Dashboard worker 线程同步等待自身。")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError as error:
            future.cancel()
            raise DashboardQueryError("Dashboard GUI 生命周期操作超时。") from error

    def cancel_pending(self, *, timeout: float = 5.0) -> None:
        """@brief 在 worker loop 取消并收敛在途查询 / Cancel and settle in-flight queries on the worker loop.

        @param timeout 最大等待秒数 / Maximum wait in seconds.
        @return 无返回值 / No return value.
        """

        if self._closed:
            return
        self.run_until_complete(self._cancel_tasks(), timeout=timeout)

    def close(self) -> None:
        """@brief 有界停止后台事件循环 / Stop the background event loop with a bounded wait.

        @return 无返回值 / No return value.
        """

        if self._closed:
            return
        self.cancel_pending()
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        """@brief worker 线程主函数 / Worker-thread main function.

        @return 无返回值 / No return value.
        """

        asyncio.set_event_loop(self._loop)
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()

    async def _cancel_tasks(self) -> None:
        """@brief 在所属 loop 中取消除当前任务外的查询 / Cancel query tasks except the current task on the owning loop.

        @return 无返回值 / No return value.
        """

        current = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks() if task is not current]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def load_snapshot(
    runtime: DashboardRuntime,
    *,
    workspace_id: str,
    service: str | None,
    duration: timedelta,
    signal: SignalKind,
) -> GuiSnapshot:
    """@brief 并行加载卡片与当前趋势 / Load summary cards and the active trend concurrently.

    @param runtime Dashboard 运行时 / Dashboard runtime.
    @param workspace_id 工作区 / Workspace identifier.
    @param service 可选服务过滤 / Optional service filter.
    @param duration 查询窗口 / Query duration.
    @param signal 当前黄金信号 / Active golden signal.
    @return 原子刷新快照 / Atomic refresh snapshot.
    """

    principal = runtime.local_principal()
    scope = runtime.workspace_scope(workspace_id)
    end_at = datetime.now(UTC)
    window = TimeWindow.ending_at(end_at, duration)
    overview, trend = await asyncio.gather(
        runtime.queries.overview(principal, scope, window=window, service=service),
        runtime.queries.trends(principal, scope, signal, window=window, service=service),
    )
    return GuiSnapshot(overview=overview, trend=trend)


def create_gui_window(
    runtime: DashboardRuntime,
    *,
    modules: _GuiModules | None = None,
) -> Any:
    """@brief 创建产品化 Qt 窗口 / Create the productized Qt window.

    @param runtime 已组合 Dashboard 运行时 / Composed Dashboard runtime.
    @param modules 可选已加载 GUI 模块 / Optional preloaded GUI modules.
    @return 尚未 show 的 QMainWindow / QMainWindow not yet shown.
    """

    loaded = modules or _load_gui_modules()
    core = loaded.core
    widgets = loaded.widgets

    class Bridge(core.QObject):  # type: ignore[misc, name-defined]
        """@brief 将后台结果排队到 Qt UI 线程 / Queue worker results onto the Qt UI thread."""

        completed = core.pyqtSignal(object)
        failed = core.pyqtSignal(object)

    worker = AsyncQueryWorker()
    bridge = Bridge()
    window = widgets.QMainWindow()
    demo_mode = runtime.settings.database.mode == "memory"
    mode_prefix = "DEMO · " if demo_mode else ""
    window.setWindowTitle(f"{mode_prefix}AI Job Workspace · Reliability Dashboard")
    window.resize(1280, 820)
    window.setStyleSheet(_GUI_STYLE)

    root = widgets.QWidget(window)
    root_layout = widgets.QVBoxLayout(root)
    root_layout.setContentsMargins(22, 20, 22, 20)
    root_layout.setSpacing(14)

    title = widgets.QLabel("Reliability Overview", root)
    title.setObjectName("title")
    subtitle_text = "SLO · Error budget · Google SRE golden signals · Diagnostic freshness"
    if demo_mode:
        subtitle_text = f"DEMO EMPTY ADAPTER · {subtitle_text}"
    subtitle = widgets.QLabel(subtitle_text, root)
    subtitle.setObjectName("subtitle")
    root_layout.addWidget(title)
    root_layout.addWidget(subtitle)

    controls = widgets.QHBoxLayout()
    workspace_input = widgets.QLineEdit(runtime.settings.default_workspace_id, root)
    workspace_input.setPlaceholderText("Workspace ID")
    service_input = widgets.QLineEdit(root)
    service_input.setPlaceholderText("全部服务")
    window_select = widgets.QComboBox(root)
    window_select.addItems(list(_WINDOWS))
    window_select.setCurrentText("1 小时")
    signal_select = widgets.QComboBox(root)
    for label, signal in (
        ("流量", SignalKind.TRAFFIC),
        ("延迟", SignalKind.LATENCY),
        ("错误", SignalKind.ERRORS),
        ("饱和度", SignalKind.SATURATION),
    ):
        signal_select.addItem(label, signal.value)
    refresh_button = widgets.QPushButton("刷新", root)
    export_button = widgets.QPushButton("导出交互报告", root)
    export_button.setEnabled(False)
    for control in (
        workspace_input,
        service_input,
        window_select,
        signal_select,
        refresh_button,
        export_button,
    ):
        controls.addWidget(control)
    root_layout.addLayout(controls)

    cards = widgets.QHBoxLayout()
    health_value = _add_card(widgets, cards, "健康状态")
    traffic_value = _add_card(widgets, cards, "请求数")
    error_value = _add_card(widgets, cards, "错误率")
    latency_value = _add_card(widgets, cards, "p95 延迟")
    burn_value = _add_card(widgets, cards, "错误预算燃烧")
    freshness_value = _add_card(widgets, cards, "遥测新鲜度")
    root_layout.addLayout(cards)

    figure = create_matplotlib_figure()
    canvas = loaded.backend.FigureCanvasQTAgg(figure)
    root_layout.addWidget(canvas, stretch=3)

    service_table = widgets.QTableWidget(0, 7, root)
    service_table.setHorizontalHeaderLabels(
        ["服务", "状态", "请求", "错误率", "p95(ms)", "峰值饱和度", "最新观测"]
    )
    service_table.horizontalHeader().setStretchLastSection(True)
    service_table.setEditTriggers(widgets.QAbstractItemView.EditTrigger.NoEditTriggers)
    service_table.setSelectionBehavior(widgets.QAbstractItemView.SelectionBehavior.SelectRows)
    root_layout.addWidget(service_table, stretch=2)

    initial_status = (
        "演示模式 · memory 适配器不代表持久化运行态"
        if demo_mode
        else "准备就绪 · 查询在后台 worker 执行"
    )
    status = widgets.QLabel(initial_status, root)
    status.setObjectName("subtitle")
    root_layout.addWidget(status)
    window.setCentralWidget(root)
    state: dict[str, TrendReport | None] = {"trend": None}

    def refresh() -> None:
        """@brief 向后台 worker 提交一次刷新 / Submit one refresh to the background worker.

        @return 无返回值 / No return value.
        """

        workspace_id = workspace_input.text().strip()
        if not workspace_id:
            status.setText("工作区不能为空。")
            return
        refresh_button.setEnabled(False)
        status.setText("正在查询完整窗口聚合…")
        duration = _WINDOWS[window_select.currentText()]
        service = service_input.text().strip() or None
        signal = SignalKind(str(signal_select.currentData()))
        worker.submit(
            load_snapshot(
                runtime,
                workspace_id=workspace_id,
                service=service,
                duration=duration,
                signal=signal,
            ),
            lambda snapshot: bridge.completed.emit(("refresh", snapshot)),
            bridge.failed.emit,
        )

    def export() -> None:
        """@brief 在后台生成用户选择的独立 Plotly 报告 / Generate a user-selected standalone Plotly report in the worker.

        @return 无返回值 / No return value.
        """

        report = state["trend"]
        if report is None:
            return
        selected, _ = widgets.QFileDialog.getSaveFileName(
            window,
            "导出交互式趋势报告",
            "dashboard-trend.html",
            "HTML (*.html)",
        )
        if not selected:
            return
        export_button.setEnabled(False)
        status.setText("正在生成独立 Plotly 报告…")
        worker.submit(
            _write_report(report, Path(selected)),
            lambda path: bridge.completed.emit(("export", path)),
            bridge.failed.emit,
        )

    def completed(result: object) -> None:
        """@brief 在 UI 线程应用后台结果 / Apply a worker result on the UI thread.

        @param result 带标签的结果 / Tagged result.
        @return 无返回值 / No return value.
        """

        if not isinstance(result, tuple) or len(result) != 2:
            failed(DashboardQueryError("Dashboard GUI worker 返回无效结果。"))
            return
        action, payload = result
        if action == "export" and isinstance(payload, Path):
            export_button.setEnabled(True)
            status.setText(f"交互报告已写入 {payload}")
            return
        if action != "refresh" or not isinstance(payload, GuiSnapshot):
            failed(DashboardQueryError("Dashboard GUI worker 返回无效快照。"))
            return
        overview = payload.overview
        state["trend"] = payload.trend
        health_value.setText(overview.health.value.upper())
        traffic_value.setText(f"{overview.request_count:,.0f}")
        error_value.setText(_format_ratio(overview.slo.error_rate))
        p95_values = [item.latency_p95_ms for item in overview.services if item.latency_p95_ms is not None]
        latency_value.setText("-" if not p95_values else f"{max(p95_values):,.1f} ms")
        burn_value.setText("-" if overview.slo.burn_rate is None else f"{overview.slo.burn_rate:.2f}×")
        freshness_prefix = (
            "历史 " if overview.freshness.mode is FreshnessMode.HISTORICAL else ""
        )
        freshness_value.setText(
            "无数据"
            if overview.freshness.lag_seconds is None
            else f"{freshness_prefix}{overview.freshness.lag_seconds:.1f} s"
        )
        _fill_service_table(widgets, service_table, overview)
        draw_matplotlib_trend(figure, payload.trend)
        canvas.draw_idle()
        refresh_button.setEnabled(True)
        export_button.setEnabled(True)
        status.setText(
            f"已更新 · {overview.window.start_at:%Y-%m-%d %H:%M} → "
            f"{overview.window.end_at:%H:%M} UTC · {len(overview.services)} 个服务"
        )

    def failed(error: object) -> None:
        """@brief 在 UI 线程呈现受控失败 / Present a controlled failure on the UI thread.

        @param error 后台错误 / Background error.
        @return 无返回值 / No return value.
        """

        refresh_button.setEnabled(True)
        export_button.setEnabled(state["trend"] is not None)
        if isinstance(error, DashboardError):
            status.setText(f"查询失败 · {error}")
        else:
            status.setText("查询失败 · 请查看受控日志")

    queued = core.Qt.ConnectionType.QueuedConnection
    bridge.completed.connect(completed, type=queued)
    bridge.failed.connect(failed, type=queued)
    refresh_button.clicked.connect(refresh)
    export_button.clicked.connect(export)
    window._dashboard_worker = worker
    window._dashboard_bridge = bridge
    core.QTimer.singleShot(0, refresh)
    return window


async def _write_report(report: TrendReport, path: Path) -> Path:
    """@brief 在线程池写出 Plotly 报告 / Write a Plotly report in a thread pool.

    @param report 趋势报告 / Trend report.
    @param path 目标路径 / Target path.
    @return 最终路径 / Final path.
    """

    return await asyncio.to_thread(write_plotly_report, report, path)


def _add_card(widgets: Any, layout: Any, label: str) -> Any:
    """@brief 添加一个一致的指标卡片 / Add one consistent metric card.

    @param widgets QtWidgets 模块 / QtWidgets module.
    @param layout 父布局 / Parent layout.
    @param label 卡片标题 / Card title.
    @return 数值 QLabel / Value QLabel.
    """

    card = widgets.QFrame()
    card.setObjectName("card")
    card_layout = widgets.QVBoxLayout(card)
    title = widgets.QLabel(label, card)
    title.setObjectName("cardTitle")
    value = widgets.QLabel("-", card)
    value.setObjectName("cardValue")
    card_layout.addWidget(title)
    card_layout.addWidget(value)
    layout.addWidget(card)
    return value


def _fill_service_table(widgets: Any, table: Any, report: DashboardOverview) -> None:
    """@brief 填充服务可靠性表 / Populate the service-reliability table.

    @param widgets QtWidgets 模块 / QtWidgets module.
    @param table QTableWidget / QTableWidget.
    @param report Overview 报告 / Overview report.
    @return 无返回值 / No return value.
    """

    table.setRowCount(len(report.services))
    for row_index, service in enumerate(report.services):
        values = (
            service.service,
            service.health.value,
            f"{service.request_count:,.0f}",
            _format_ratio(service.error_rate),
            "-" if service.latency_p95_ms is None else f"{service.latency_p95_ms:,.1f}",
            _format_ratio(service.saturation_max),
            f"{service.latest_observed_at:%H:%M:%S}",
        )
        for column_index, value in enumerate(values):
            table.setItem(row_index, column_index, widgets.QTableWidgetItem(value))
    table.resizeColumnsToContents()


def _format_ratio(value: float | None) -> str:
    """@brief 将比例格式化为百分比 / Format a ratio as a percentage.

    @param value 可空比例 / Nullable ratio.
    @return 百分比文本 / Percentage text.
    """

    return "-" if value is None else f"{value * 100:.2f}%"


def _load_gui_modules() -> _GuiModules:
    """@brief 延迟加载 Qt 与 Matplotlib Qt backend / Lazily load Qt and the Matplotlib Qt backend.

    @return GUI 模块集合 / GUI module bundle.
    """

    try:
        return _GuiModules(
            core=import_module("PyQt6.QtCore"),
            widgets=import_module("PyQt6.QtWidgets"),
            backend=import_module("matplotlib.backends.backend_qtagg"),
        )
    except ModuleNotFoundError as error:
        raise DashboardDependencyError(
            "Dashboard GUI 需要可选依赖 PyQt6 与 matplotlib；请安装项目的 gui extra。"
        ) from error


def close_runtime_in_worker(
    worker: AsyncQueryWorker,
    runtime: AsyncClosable,
    *,
    timeout: float = 5.0,
) -> None:
    """@brief 在拥有 asyncpg pool 的 loop 中有序关闭运行时 / Orderly close a runtime on the loop owning its asyncpg pool.

    @param worker 拥有连接池使用 loop 的 worker / Worker owning the pool-use loop.
    @param runtime 待关闭运行时 / Runtime to close.
    @param timeout 每阶段最大等待秒数 / Maximum wait per phase in seconds.
    @return 无返回值 / No return value.

    @note 顺序固定为取消查询、关闭 engine、停止 loop，避免 asyncpg ``attached to a different
    loop`` 与 pending-task 泄漏。/ The fixed order is cancel queries, dispose the engine, then stop
    the loop, avoiding asyncpg cross-loop errors and pending-task leaks.
    """

    worker.cancel_pending(timeout=timeout)
    worker.run_until_complete(runtime.aclose(), timeout=timeout)


def run_gui(
    runtime: DashboardRuntime | None = None,
    *,
    config_path: str | Path = "config.jsonc",
) -> int:
    """@brief 运行 Qt 事件循环 / Run the Qt event loop.

    @param runtime 可选外部运行时 / Optional externally owned runtime.
    @param config_path 未注入运行时时使用的根 JSONC 配置 / Root JSONC config used without an injected runtime.
    @return Qt 进程退出码 / Qt process exit code.
    """

    modules = _load_gui_modules()
    owns_runtime = runtime is None
    resolved_runtime = runtime or build_runtime(config_path=config_path)
    application = modules.widgets.QApplication.instance() or modules.widgets.QApplication(
        [sys.argv[0]]
    )
    worker: AsyncQueryWorker | None = None
    try:
        window = create_gui_window(resolved_runtime, modules=modules)
        candidate = getattr(window, "_dashboard_worker", None)
        if isinstance(candidate, AsyncQueryWorker):
            worker = candidate
        window.show()
        return int(application.exec())
    finally:
        if worker is not None:
            try:
                if owns_runtime:
                    close_runtime_in_worker(worker, resolved_runtime)
            finally:
                worker.close()
        elif owns_runtime:
            asyncio.run(resolved_runtime.aclose())


def build_parser() -> argparse.ArgumentParser:
    """@brief 构建无副作用 GUI 参数解析器 / Build the side-effect-free GUI argument parser.

    @return 支持标准 help 与配置覆盖的 parser / Parser supporting standard help and config override.
    """

    parser = argparse.ArgumentParser(
        prog="dashboard-gui",
        description="启动只读 Reliability Dashboard 桌面界面。",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.jsonc"),
        help="根 JSONC 配置路径（默认：config.jsonc）。",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """@brief GUI console script 入口 / GUI console-script entry point.

    @param argv 不含程序名的可选参数 / Optional arguments excluding the program name.
    @return 进程退出码 / Process exit code.
    """

    arguments = build_parser().parse_args(argv)
    try:
        return run_gui(config_path=arguments.config)
    except DashboardDependencyError as error:
        print(f"dashboard-gui: {error}", file=sys.stderr)
        return 2
    except DashboardError as error:
        print(f"dashboard-gui: {error}", file=sys.stderr)
        return 1


__all__ = [
    "AsyncQueryWorker",
    "GuiSnapshot",
    "build_parser",
    "close_runtime_in_worker",
    "create_gui_window",
    "load_snapshot",
    "main",
    "run_gui",
]
