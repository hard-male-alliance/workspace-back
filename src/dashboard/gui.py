"""可选 PyQt6 Dashboard 图形界面（GUI）适配器。"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any

from .composition import DashboardApplication, create_dashboard_application
from .errors import DashboardDependencyError, DashboardError


def create_gui_window(application: DashboardApplication) -> Any:
    """@brief 创建调用 DashboardService 的最小 PyQt6 主窗口。

    @param application: 已组合 DashboardApplication；窗口不会自行关闭其资源。
    @return: 已配置但尚未 show 的 QMainWindow。

    @note PyQt6 是可选依赖（optional dependency）。本函数采用延迟导入，使
    headless CLI/API 环境不需要安装 Qt。
    """

    try:
        widgets = import_module("PyQt6.QtWidgets")
    except ModuleNotFoundError as error:
        raise DashboardDependencyError(
            "Dashboard GUI 需要可选依赖 PyQt6；CLI 与 API 不需要安装它。"
        ) from error

    window = widgets.QMainWindow()
    window.setWindowTitle("AI Job Workspace Dashboard")
    window.resize(900, 620)
    root = widgets.QWidget(window)
    layout = widgets.QVBoxLayout(root)
    form = widgets.QFormLayout()
    workspace_input = widgets.QLineEdit(root)
    workspace_input.setPlaceholderText("例如 ws_01J...")
    window_minutes = widgets.QSpinBox(root)
    window_minutes.setRange(
        1,
        max(1, int(application.settings.max_window.total_seconds() // 60)),
    )
    window_minutes.setValue(max(1, int(application.settings.default_window.total_seconds() // 60)))
    form.addRow("工作区 ID", workspace_input)
    form.addRow("查询窗口（分钟）", window_minutes)
    layout.addLayout(form)

    actions = widgets.QHBoxLayout()
    refresh_button = widgets.QPushButton("刷新概览", root)
    status = widgets.QLabel("请输入工作区 ID 后刷新。", root)
    actions.addWidget(refresh_button)
    actions.addWidget(status)
    layout.addLayout(actions)

    output = widgets.QPlainTextEdit(root)
    output.setReadOnly(True)
    layout.addWidget(output)
    window.setCentralWidget(root)

    def _refresh() -> None:
        """将 GUI 输入转交给共享 DashboardService 并渲染结果。"""

        workspace_id = workspace_input.text().strip()
        if not workspace_id:
            status.setText("工作区 ID 不能为空。")
            return

        refresh_button.setEnabled(False)
        status.setText("正在读取可观测性摘要…")
        try:
            end_at = datetime.now(UTC)
            overview = asyncio.run(
                application.service.overview(
                    application.scope_for_local_operator(workspace_id),
                    start_at=end_at - timedelta(minutes=window_minutes.value()),
                    end_at=end_at,
                    max_samples=None,
                )
            )
        except (DashboardError, RuntimeError) as error:
            status.setText(f"读取失败：{error}")
        else:
            output.setPlainText(
                json.dumps(overview.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
            )
            status.setText(f"刷新成功：{overview.health.value}")
        finally:
            refresh_button.setEnabled(True)

    refresh_button.clicked.connect(_refresh)
    return window


def run_gui(application: DashboardApplication | None = None) -> int:
    """@brief 启动可选 PyQt6 GUI 的进程入口（GUI process entrypoint）。

    @param application: 可选已组合应用；省略时按根配置创建仓库并由本函数关闭。
    @return: Qt 事件循环返回的退出码。
    """

    try:
        widgets = import_module("PyQt6.QtWidgets")
    except ModuleNotFoundError as error:
        raise DashboardDependencyError(
            "Dashboard GUI 需要可选依赖 PyQt6；请安装 dashboard GUI extra。"
        ) from error

    owns_application = application is None
    resolved_application = create_dashboard_application() if application is None else application
    qt_application = widgets.QApplication.instance() or widgets.QApplication(sys.argv)
    window = create_gui_window(resolved_application)
    window.show()
    try:
        return int(qt_application.exec())
    finally:
        if owns_application:
            asyncio.run(resolved_application.aclose())


def main() -> int:
    """@brief 运行可选 Dashboard GUI console entrypoint / Run the optional Dashboard GUI console entrypoint.

    @return 成功时返回 Qt 进程退出码；缺少 GUI extra 时返回 ``2``。
    / Returns the Qt process exit code on success and ``2`` when the GUI extra is absent.

    @note 此入口在调用 ``run_gui`` 前不会导入 PyQt6，因此 headless 安装仍可安全解析
    ``workspace-dashboard-gui``。用户只有显式安装 ``gui`` extra 并执行该命令时才会
    加载 Qt。
    / This entrypoint does not import PyQt6 before calling ``run_gui``, so headless installations
    can safely resolve ``workspace-dashboard-gui``. Qt is loaded only when a user explicitly
    installs the ``gui`` extra and invokes this command.
    """

    try:
        return run_gui()
    except DashboardDependencyError as error:
        print(f"dashboard-gui: {error}", file=sys.stderr)
        return 2
    except DashboardError as error:
        print(f"dashboard-gui: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover - standard module entrypoint
    raise SystemExit(main())


__all__ = ["create_gui_window", "main", "run_gui"]
