"""@brief Dashboard 分层应用、CLI、API 与 GUI worker 流程测试 / Dashboard layered application, CLI, API, and GUI-worker flow tests."""

from __future__ import annotations

import asyncio
import io
import json
import sys
import threading
import time
import tomllib
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn

import pytest
from fastapi.testclient import TestClient

from conftest import PROJECT_ROOT
from dashboard.application.errors import DashboardDependencyError, DashboardQueryError
from dashboard.application.ports import (
    DiagnosticEventRow,
    EventReadRequest,
    OverviewReadRequest,
    ServiceSignalRow,
    SystemHealthReadRequest,
    SystemHealthRow,
    TrendReadRequest,
    TrendSignalRow,
)
from dashboard.bootstrap import DashboardRuntime, build_runtime
from dashboard.domain.model import (
    HealthStatus,
    OperatorPrincipal,
    ServiceLevelObjective,
    SignalKind,
    WorkspaceScope,
)
from dashboard.infrastructure.config import (
    AccessMode,
    DashboardAccessSettings,
    DashboardApiSettings,
    DashboardDatabaseSettings,
    DashboardQuerySettings,
    DashboardSettings,
)
from dashboard.interfaces import gui as dashboard_gui
from dashboard.interfaces import visualization
from dashboard.interfaces.api import create_fastapi_app
from dashboard.interfaces.cli import async_main


class RecordingReadStore:
    """@brief 记录请求并返回数据库已聚合行的测试读存储 / Test read store recording requests and returning database-aggregated rows."""

    def __init__(self, observed_at: datetime) -> None:
        """@brief 创建确定性读存储 / Create a deterministic read store.

        @param observed_at 最近采集时刻 / Latest collection time.
        @return 新测试存储 / New test store.
        """

        self.overview_requests: list[OverviewReadRequest] = []
        self.trend_requests: list[TrendReadRequest] = []
        self.event_requests: list[EventReadRequest] = []
        self.system_health_requests: list[SystemHealthReadRequest] = []
        self._observed_at = observed_at

    async def fetch_overview(self, request: OverviewReadRequest) -> Sequence[ServiceSignalRow]:
        """@brief 返回一行完整窗口聚合 / Return one complete-window aggregate.

        @param request Overview 请求 / Overview request.
        @return 服务聚合 / Service aggregate.
        """

        self.overview_requests.append(request)
        return (
            ServiceSignalRow(
                service="backend",
                request_count=100.0,
                error_count=2.0,
                latency_p50_ms=80.0,
                latency_p95_ms=250.0,
                latency_p99_ms=500.0,
                saturation_mean=0.40,
                saturation_max=0.85,
                sample_count=42,
                max_collection_lag_seconds=1.0,
                latest_observed_at=self._observed_at,
            ),
        )

    async def fetch_trends(self, request: TrendReadRequest) -> Sequence[TrendSignalRow]:
        """@brief 返回两个 SQL 分桶点 / Return two SQL-bucketed points.

        @param request 趋势请求 / Trend request.
        @return 趋势行 / Trend rows.
        """

        self.trend_requests.append(request)
        return (
            TrendSignalRow(
                bucket_start=request.window.start_at,
                service="backend",
                request_count=40.0,
                error_count=1.0,
                latency_p50_ms=70.0,
                latency_p95_ms=220.0,
                latency_p99_ms=450.0,
                saturation_mean=0.35,
                saturation_max=0.70,
            ),
            TrendSignalRow(
                bucket_start=request.window.end_at - timedelta(seconds=request.bucket_seconds),
                service="backend",
                request_count=60.0,
                error_count=1.0,
                latency_p50_ms=90.0,
                latency_p95_ms=280.0,
                latency_p99_ms=550.0,
                saturation_mean=0.45,
                saturation_max=0.85,
            ),
        )

    async def fetch_recent_events(
        self,
        request: EventReadRequest,
    ) -> Sequence[DiagnosticEventRow]:
        """@brief 返回 canonical v2 log 事件 / Return one canonical-v2 log event.

        @param request 事件请求 / Event request.
        @return 诊断事件 / Diagnostic event.
        """

        self.event_requests.append(request)
        return (
            DiagnosticEventRow(
                occurred_at=self._observed_at - timedelta(seconds=2),
                observed_at=self._observed_at,
                source="backend",
                service="backend",
                kind="log",
                name="aiws.http.request.failed",
                severity_number=17,
                severity_text="ERROR",
                value=None,
                unit=None,
                duration_ms=None,
                span_status=None,
                request_id="req-test",
                trace_id="trace-test",
                span_id="span-test",
                attributes={"route": "/api/v1/jobs"},
            ),
        )

    async def fetch_system_health(
        self,
        request: SystemHealthReadRequest,
    ) -> SystemHealthRow:
        """@brief 返回确定性全局管线快照 / Return deterministic global pipeline health.

        @param request 系统健康请求 / System-health request.
        @return 警告级管线快照 / Warning-level pipeline snapshot.
        """

        self.system_health_requests.append(request)
        return SystemHealthRow(
            occurred_at=self._observed_at - timedelta(seconds=1),
            observed_at=self._observed_at,
            severity_number=13,
            severity_text="WARNING",
            accepted_count=1_000,
            dropped_count=2,
            write_failure_count=1,
            output_dropped_count=3,
        )


def _settings(
    *,
    access_mode: AccessMode = "mock",
    enabled: bool = True,
) -> DashboardSettings:
    """@brief 创建测试 Dashboard 配置 / Build test Dashboard settings.

    @param access_mode mock 或 operator_token / Mock or operator-token mode.
    @param enabled 是否启用 Dashboard 数据面 / Whether the Dashboard data plane is enabled.
    @return 不可变配置 / Immutable settings.
    """

    return DashboardSettings(
        environment="test",
        enabled=enabled,
        default_workspace_id="ws-dashboard-flow",
        database=DashboardDatabaseSettings(mode="memory"),
        query=DashboardQuerySettings(
            default_window=timedelta(hours=1),
            max_window=timedelta(days=7),
            statement_timeout_ms=3_000,
            freshness_target=timedelta(minutes=2),
            target_points=120,
            max_event_limit=500,
        ),
        access=DashboardAccessSettings(
            mode=access_mode,
            operator_id="operator-klee",
            token=(
                "correct-horse-battery-staple"
                if access_mode == "operator_token"
                else None
            ),
        ),
        api=DashboardApiSettings(prefix="/dashboard/v1"),
        objective=ServiceLevelObjective(
            availability_target=0.99,
            latency_target=0.95,
            latency_threshold_ms=1_000.0,
        ),
    )


def _runtime(
    *,
    access_mode: AccessMode = "mock",
    enabled: bool = True,
) -> tuple[DashboardRuntime, RecordingReadStore, datetime]:
    """@brief 组装固定时钟 Dashboard runtime / Compose a fixed-clock Dashboard runtime.

    @param access_mode mock 或 operator_token / Mock or operator-token mode.
    @param enabled 是否启用 Dashboard 数据面 / Whether the Dashboard data plane is enabled.
    @return runtime、记录存储与当前时刻 / Runtime, recording store, and current time.
    """

    now = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    store = RecordingReadStore(now - timedelta(seconds=30))
    runtime = build_runtime(
        settings=_settings(access_mode=access_mode, enabled=enabled),
        store=store,
        clock=lambda: now,
    )
    return runtime, store, now


@pytest.mark.asyncio
async def test_application_keeps_principal_and_workspace_scope_separate() -> None:
    """@brief 应用层分别表达主体与租户范围 / Application layer represents principal and tenant scope separately."""

    runtime, store, now = _runtime()
    principal = runtime.local_principal()
    scope = runtime.workspace_scope()
    report = await runtime.queries.overview(principal, scope)

    assert isinstance(principal, OperatorPrincipal)
    assert isinstance(scope, WorkspaceScope)
    assert report.principal.operator_id == "operator-klee"
    assert report.scope.workspace_id == "ws-dashboard-flow"
    assert report.window.start_at == now - timedelta(hours=1)
    assert report.health is HealthStatus.DEGRADED
    assert report.slo.error_rate == pytest.approx(0.02)
    assert report.slo.burn_rate == pytest.approx(2.0)
    assert report.slo.budget_remaining_ratio == pytest.approx(1.0 - 2.0 / (30 * 24))
    assert report.freshness.lag_seconds == 30.0
    assert store.overview_requests[0].scope.workspace_id == "ws-dashboard-flow"


@pytest.mark.asyncio
async def test_zero_argument_cli_defaults_to_overview_and_pipe_json() -> None:
    """@brief 零参数 CLI 对管道输出默认工作区 Overview JSON / Zero-argument CLI emits default-workspace overview JSON to a pipe."""

    runtime, store, _ = _runtime()
    output = io.StringIO()
    error = io.StringIO()

    assert await async_main([], runtime=runtime, stdout=output, stderr=error) == 0
    payload = json.loads(output.getvalue())
    assert payload["data_source"] == "demo_empty_adapter"
    assert payload["scope"] == {"workspace_id": "ws-dashboard-flow"}
    assert payload["principal"] == {"operator_id": "operator-klee"}
    assert payload["slo"]["error_budget_burn_rate"] == pytest.approx(2.0)
    assert payload["freshness"]["lag_seconds"] == 30.0
    assert len(store.overview_requests) == 1
    assert error.getvalue() == ""


@pytest.mark.parametrize(
    ("view", "expected_call"),
    (
        ("overview", "overview"),
        ("services", "overview"),
        ("traffic", "trend"),
        ("latency", "trend"),
        ("errors", "trend"),
        ("saturation", "trend"),
        ("diagnostics", "event"),
        ("frontend", "event"),
        ("health", "health"),
    ),
)
@pytest.mark.asyncio
async def test_cli_exposes_discoverable_views(view: str, expected_call: str) -> None:
    """@brief 九个视图都映射到有界应用用例 / All nine views map to bounded application use cases.

    @param view CLI 视图 / CLI view.
    @param expected_call 预期读存储方法 / Expected read-store method.
    """

    runtime, store, _ = _runtime()
    output = io.StringIO()
    assert (
        await async_main(
            [view, "--since", "30m", "--json"],
            runtime=runtime,
            stdout=output,
            stderr=io.StringIO(),
        )
        == 0
    )
    json.loads(output.getvalue())
    counts = {
        "overview": len(store.overview_requests),
        "trend": len(store.trend_requests),
        "event": len(store.event_requests),
        "health": len(store.system_health_requests),
    }
    assert counts[expected_call] == 1


@pytest.mark.asyncio
async def test_frontend_view_applies_the_product_service_filter_heuristically() -> None:
    """@brief frontend 视图无需用户记住内部服务名 / The frontend view hides the internal service-name convention."""

    runtime, store, _ = _runtime()
    assert (
        await async_main(
            ["frontend", "--json"],
            runtime=runtime,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )
        == 0
    )
    assert store.event_requests[0].service == "frontend.browser"


@pytest.mark.asyncio
async def test_table_output_uses_human_readable_sre_units() -> None:
    """@brief --table 输出应呈现 SRE 单位与中文空态 / ``--table`` output presents SRE units and Chinese states."""

    runtime, _, _ = _runtime()
    output = io.StringIO()
    assert await async_main(["latency", "--table"], runtime=runtime, stdout=output) == 0
    rendered = output.getvalue()
    assert "p95(ms)" in rendered
    assert "backend" in rendered
    assert "250.00" in rendered or "220.00" in rendered


@pytest.mark.asyncio
async def test_cli_parse_errors_use_injected_stderr() -> None:
    """@brief CLI 解析错误不应绕过可注入 stderr / CLI parse errors do not bypass injected stderr."""

    runtime, _, _ = _runtime()
    error = io.StringIO()
    assert await async_main(["traffic", "--since", "yesterday"], runtime=runtime, stderr=error) == 2
    assert "--since" in error.getvalue()


def test_api_exposes_overview_trends_and_events() -> None:
    """@brief API 三个读端点复用同一应用服务 / All three API read endpoints reuse the same application service."""

    runtime, store, _ = _runtime()
    with TestClient(create_fastapi_app(runtime)) as client:
        overview = client.get("/dashboard/v1/overview")
        assert overview.status_code == 200, overview.text
        assert overview.json()["scope"] == {"workspace_id": "ws-dashboard-flow"}

        trends = client.get("/dashboard/v1/trends", params={"signal": "latency", "since": "1h"})
        assert trends.status_code == 200, trends.text
        assert trends.json()["signal"] == "latency"

        events = client.get("/dashboard/v1/events", params={"limit": 10})
        assert events.status_code == 200, events.text
        assert events.json()["events"][0]["span_id"] == "span-test"
        assert events.json()["events"][0]["source"] == "backend"

        health = client.get("/dashboard/v1/system-health")
        assert health.status_code == 200, health.text
        assert health.json()["scope"] == {"kind": "system"}
        assert health.json()["dropped_count"] == 2

    assert len(store.overview_requests) == 1
    assert len(store.trend_requests) == 1
    assert store.trend_requests[0].signal is SignalKind.LATENCY
    assert store.event_requests[0].limit == 10
    assert len(store.system_health_requests) == 1


def test_api_authenticates_principal_without_merging_workspace_scope() -> None:
    """@brief HTTP token 只认证主体，workspace 仍是独立查询范围 / HTTP token authenticates the principal while workspace remains a separate query scope."""

    runtime, _, _ = _runtime(access_mode="operator_token")
    with TestClient(create_fastapi_app(runtime)) as client:
        assert client.get("/dashboard/v1/overview").status_code == 401
        response = client.get(
            "/dashboard/v1/overview",
            params={"workspace_id": "ws-explicit"},
            headers={"X-Dashboard-Operator-Token": "correct-horse-battery-staple"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["principal"] == {"operator_id": "operator-klee"}
        assert response.json()["scope"] == {"workspace_id": "ws-explicit"}


def test_system_health_obeys_disabled_runtime_gate() -> None:
    """@brief system-health 不得绕过禁用运行时的可用性门禁 / System-health cannot bypass the disabled-runtime availability gate."""

    runtime, store, _ = _runtime(enabled=False)
    with TestClient(create_fastapi_app(runtime)) as client:
        probe = client.get("/dashboard/v1/healthz")
        response = client.get("/dashboard/v1/system-health")

    assert probe.status_code == 200
    assert probe.json()["status"] == "disabled"
    assert response.status_code == 422
    assert store.system_health_requests == []


def test_system_health_obeys_closed_runtime_gate() -> None:
    """@brief system-health 不得在运行时关闭后读取存储 / System-health cannot read the store after runtime closure."""

    runtime, store, _ = _runtime()
    asyncio.run(runtime.aclose())
    with TestClient(create_fastapi_app(runtime)) as client:
        response = client.get("/dashboard/v1/system-health")

    assert response.status_code == 422
    assert store.system_health_requests == []
    with pytest.raises(DashboardQueryError, match="已关闭"):
        runtime.authenticate_http(None)


def test_gui_worker_runs_coroutines_off_the_calling_thread() -> None:
    """@brief GUI worker 提交不阻塞调用线程 / GUI worker submission does not block the calling thread."""

    worker = dashboard_gui.AsyncQueryWorker()
    completed = threading.Event()
    results: list[int] = []
    errors: list[Exception] = []

    async def delayed() -> int:
        """@brief 模拟慢查询 / Simulate a slow query.

        @return 确定值 / Deterministic value.
        """

        await asyncio.sleep(0.05)
        return 42

    started = time.monotonic()
    def succeeded(value: int) -> None:
        """@brief 记录后台成功 / Record worker success.

        @param value 后台结果 / Worker result.
        @return 无返回值 / No return value.
        """

        results.append(value)
        completed.set()

    def failed(error: Exception) -> None:
        """@brief 记录后台失败 / Record worker failure.

        @param error 后台异常 / Worker exception.
        @return 无返回值 / No return value.
        """

        errors.append(error)
        completed.set()

    worker.submit(delayed(), succeeded, failed)
    submit_elapsed = time.monotonic() - started
    assert submit_elapsed < 0.04
    assert completed.wait(timeout=1)
    worker.close()
    assert results == [42]
    assert errors == []


def test_owned_runtime_closes_on_the_worker_event_loop() -> None:
    """@brief asyncpg 运行时应在首次使用它的 worker loop/thread 关闭 / An asyncpg runtime closes on the worker loop and thread that used it."""

    class RecordingRuntime:
        """@brief 记录 aclose 执行位置的 fake runtime / Fake runtime recording where ``aclose`` executes."""

        def __init__(self) -> None:
            """@brief 初始化记录字段 / Initialize recording fields.

            @return 新 fake runtime / New fake runtime.
            """

            self.closed_thread: int | None = None
            self.closed_loop: asyncio.AbstractEventLoop | None = None

        async def aclose(self) -> None:
            """@brief 记录资源关闭 loop 与线程 / Record the resource-close loop and thread.

            @return 无返回值 / No return value.
            """

            self.closed_thread = threading.get_ident()
            self.closed_loop = asyncio.get_running_loop()

    async def record_use() -> tuple[int, asyncio.AbstractEventLoop]:
        """@brief 模拟连接池首次使用 / Simulate first connection-pool use.

        @return worker 线程与 loop / Worker thread and loop.
        """

        return threading.get_ident(), asyncio.get_running_loop()

    worker = dashboard_gui.AsyncQueryWorker()
    runtime = RecordingRuntime()
    used_thread, used_loop = worker.run_until_complete(record_use())
    dashboard_gui.close_runtime_in_worker(worker, runtime)
    worker.close()

    assert runtime.closed_thread == used_thread
    assert runtime.closed_loop is used_loop


def test_gui_entrypoint_reports_missing_optional_extra(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief headless 安装得到受控 GUI 依赖错误 / Headless installations receive a controlled GUI dependency error.

    @param monkeypatch pytest monkeypatch fixture / pytest monkeypatch fixture.
    @param capsys pytest stream-capture fixture / pytest stream-capture fixture.
    """

    def missing_gui_extra(
        runtime: DashboardRuntime | None = None,
        *,
        config_path: str | Path = "config.jsonc",
    ) -> NoReturn:
        """@brief 模拟缺少 GUI extra / Simulate a missing GUI extra.

        @param runtime 可选运行时 / Optional runtime.
        @param config_path 根配置路径 / Root config path.
        @return 永不返回 / Never returns.
        """

        del runtime, config_path
        raise DashboardDependencyError("需要 PyQt6 与 matplotlib。")

    monkeypatch.setattr(dashboard_gui, "run_gui", missing_gui_extra)
    assert dashboard_gui.main([]) == 2
    assert "PyQt6" in capsys.readouterr().err


def test_gui_parser_supports_help_without_loading_optional_dependencies(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@brief GUI help 不应加载 Qt/Matplotlib / GUI help must not load Qt or Matplotlib.

    @param capsys pytest stream-capture fixture / Pytest stream-capture fixture.
    @return 无返回值 / No return value.
    """

    with pytest.raises(SystemExit) as exit_info:
        dashboard_gui.main(["--help"])
    assert exit_info.value.code == 0
    assert "dashboard-gui" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_plotly_export_uses_collision_free_atomic_temporary_files(
    tmp_path: Path,
) -> None:
    """@brief 并发导出同一报告不得共享固定 tmp 路径 / Concurrent exports must not share a fixed temporary path.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    """

    runtime, _, _ = _runtime()
    report = await runtime.queries.trends(
        runtime.local_principal(),
        runtime.workspace_scope(),
        SignalKind.LATENCY,
    )
    target = tmp_path / "trend.html"
    first, second = await asyncio.gather(
        asyncio.to_thread(visualization.write_plotly_report, report, target),
        asyncio.to_thread(visualization.write_plotly_report, report, target),
    )

    assert first == target
    assert second == target
    html = await asyncio.to_thread(target.read_text, encoding="utf-8")
    leftovers = await asyncio.to_thread(
        lambda: tuple(tmp_path.glob(".trend.html.*.tmp"))
    )
    assert "plotly" in html.lower()
    assert leftovers == ()


def test_gui_does_not_forward_dashboard_arguments_to_qt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief Dashboard 参数不得再次交给 Qt 解析 / Dashboard arguments must not be parsed again by Qt.

    @param monkeypatch pytest 替换工具 / Pytest patch helper.
    @return 无返回值 / No return value.
    """

    received: list[list[str]] = []

    class FakeApplication:
        """@brief 记录 QApplication 收到的参数 / Record arguments received by QApplication."""

        @classmethod
        def instance(cls) -> None:
            """@brief 模拟尚无 QApplication / Simulate no existing QApplication.

            @return 总为 None / Always ``None``.
            """

            return None

        def __init__(self, arguments: list[str]) -> None:
            """@brief 保存 Qt 参数 / Store Qt arguments.

            @param arguments Qt 参数 / Qt arguments.
            """

            received.append(arguments)

        def exec(self) -> int:
            """@brief 模拟成功事件循环 / Simulate a successful event loop.

            @return 零退出码 / Zero exit code.
            """

            return 0

    class FakeWindow:
        """@brief 无后台 worker 的最小窗口 / Minimal window without a background worker."""

        def show(self) -> None:
            """@brief 模拟显示窗口 / Simulate showing the window."""

    runtime, _, _ = _runtime()
    modules = SimpleNamespace(
        widgets=SimpleNamespace(QApplication=FakeApplication),
        core=None,
        backend=None,
    )
    monkeypatch.setattr(dashboard_gui, "_load_gui_modules", lambda: modules)
    monkeypatch.setattr(
        dashboard_gui,
        "create_gui_window",
        lambda _runtime, *, modules: FakeWindow(),
    )
    monkeypatch.setattr(sys, "argv", ["dashboard-gui", "--config", "custom.jsonc"])

    assert dashboard_gui.run_gui(runtime) == 0
    assert received == [["dashboard-gui"]]


def test_dashboard_console_scripts_target_layered_interfaces() -> None:
    """@brief 发布入口必须指向新组合根与 GUI interface / Published scripts target the new composition root and GUI interface."""

    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    scripts = project["project"]["scripts"]
    assert scripts["dashboard"] == "dashboard.__main__:main"
    assert scripts["dashboard-api"] == "dashboard.interfaces.api:main"
    assert scripts["dashboard-gui"] == "dashboard.interfaces.gui:main"
    assert set(scripts) == {
        "backend",
        "dashboard",
        "dashboard-api",
        "dashboard-gui",
        "dbctl",
    }
