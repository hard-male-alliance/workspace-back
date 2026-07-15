"""@brief Dashboard 应用层、CLI 与 API 基础流程测试 / Basic flow tests for Dashboard application layer, CLI, and API."""

from __future__ import annotations

import asyncio
import json
import tomllib
from datetime import UTC, datetime, timedelta
from typing import NoReturn

import pytest
from fastapi.testclient import TestClient
from pytest import CaptureFixture

from conftest import PROJECT_ROOT
from dashboard import gui as dashboard_gui
from dashboard.api import create_api
from dashboard.cli import async_main
from dashboard.composition import DashboardApplication, DashboardCompositionRoot
from dashboard.config import DashboardSettings
from dashboard.errors import DashboardConfigurationError, DashboardDependencyError
from dashboard.models import DashboardScope, HealthStatus, MetricKind, MetricSample
from dashboard.repositories import MemoryObservabilityRepository


def _dashboard_fixture_application() -> tuple[DashboardApplication, datetime, datetime]:
    """@brief 组装带两租户样本的 Dashboard 应用 / Compose a Dashboard application with two-tenant samples.

    @return application、查询起点与查询终点 / Application, query start, and query end.
    """

    observed_at = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
    samples = (
        MetricSample("ws-dashboard-flow", observed_at, "backend", MetricKind.REQUESTS, 10),
        MetricSample("ws-dashboard-flow", observed_at, "backend", MetricKind.ERRORS, 1),
        MetricSample("ws-dashboard-flow", observed_at, "backend", MetricKind.LATENCY_MS, 150),
        MetricSample("ws-dashboard-flow", observed_at, "backend", MetricKind.LATENCY_MS, 250),
        MetricSample("ws-dashboard-flow", observed_at, "backend", MetricKind.SATURATION, 0.25),
        MetricSample("ws-another-tenant", observed_at, "backend", MetricKind.REQUESTS, 999),
    )
    repository = MemoryObservabilityRepository(samples)
    application = DashboardCompositionRoot().build(
        settings=DashboardSettings(operator_id="operator-klee"),
        repository=repository,
    )
    return application, observed_at - timedelta(minutes=5), observed_at + timedelta(minutes=5)


def _rfc3339(value: datetime) -> str:
    """@brief 序列化 UTC datetime 为 CLI/API 输入 / Serialize a UTC datetime for CLI/API input.

    @param value UTC datetime / UTC datetime.
    @return RFC 3339 Z timestamp / RFC 3339 Z timestamp.
    """

    return value.isoformat().replace("+00:00", "Z")


def test_dashboard_application_layer_and_cli_emit_scoped_aggregation(
    capsys: CaptureFixture[str],
) -> None:
    """@brief 应用层与 CLI 都应读取同一 scope 的聚合结果 / Application layer and CLI must read the same scoped aggregation.

    @param capsys pytest 标准输出捕获夹具 / pytest stdout capture fixture.
    """

    application, start_at, end_at = _dashboard_fixture_application()
    scope = DashboardScope("ws-dashboard-flow", "operator-klee")
    overview = asyncio.run(application.service.overview(scope, start_at=start_at, end_at=end_at))
    assert overview.request_count == 10
    assert overview.error_count == 1
    assert overview.error_rate == 0.1
    assert overview.health is HealthStatus.CRITICAL
    assert [summary.service for summary in overview.services] == ["backend"]

    exit_code = asyncio.run(
        async_main(
            [
                "overview",
                "--workspace-id",
                "ws-dashboard-flow",
                "--actor-id",
                "operator-klee",
                "--start-at",
                _rfc3339(start_at),
                "--end-at",
                _rfc3339(end_at),
                "--output",
                "json",
            ],
            application=application,
        )
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["scope"] == {"workspace_id": "ws-dashboard-flow", "actor_id": "operator-klee"}
    assert payload["request_count"] == 10
    assert payload["error_count"] == 1
    assert payload["health"] == "critical"
    asyncio.run(application.aclose())


def test_dashboard_api_uses_the_same_application_service() -> None:
    """@brief Dashboard FastAPI 适配器应仅暴露已组合应用层的只读结果 / Dashboard FastAPI adapter must expose only composed application-layer read results."""

    application, start_at, end_at = _dashboard_fixture_application()
    api = create_api(application)
    with TestClient(api) as client:
        health_response = client.get("/dashboard/v1/healthz")
        assert health_response.status_code == 200, health_response.text
        assert health_response.json() == {
            "status": "ok",
            "component": "dashboard",
            "access_mode": "mock",
        }

        overview_response = client.get(
            "/dashboard/v1/overview",
            params={
                "workspace_id": "ws-dashboard-flow",
                "actor_id": "operator-klee",
                "start_at": _rfc3339(start_at),
                "end_at": _rfc3339(end_at),
            },
        )
        assert overview_response.status_code == 200, overview_response.text
        overview = overview_response.json()
        assert overview["scope"] == {
            "workspace_id": "ws-dashboard-flow",
            "actor_id": "operator-klee",
        }
        assert overview["request_count"] == 10
        assert overview["error_count"] == 1
        assert overview["health"] == "critical"
        assert [item["service"] for item in overview["services"]] == ["backend"]

        services_response = client.get(
            "/dashboard/v1/services",
            params={
                "workspace_id": "ws-dashboard-flow",
                "start_at": _rfc3339(start_at),
                "end_at": _rfc3339(end_at),
            },
        )
        assert services_response.status_code == 200, services_response.text
        assert services_response.json()["items"][0]["health"] == "critical"
    asyncio.run(application.aclose())


@pytest.mark.parametrize("environment", ("staging", "production"))
def test_dashboard_rejects_mock_access_outside_local_environments(environment: str) -> None:
    """@brief Dashboard 独立配置不得在非本地环境保留 mock access / Dashboard config must reject mock access outside local environments.

    @param environment 需要拒绝 mock operator access 的根环境 / Root environment that must reject mock operator access.
    @return 无返回值 / No return value.

    @note 该测试只使用 Dashboard 自己的配置服务，证明该可执行程序不会依赖 backend
    对 production + memory 组合的拒绝。
    / This test uses only Dashboard's configuration service, proving that this executable does not
    rely on backend rejecting a production-plus-memory combination.
    """

    with pytest.raises(DashboardConfigurationError, match="mock 仅允许 development/test"):
        DashboardSettings.from_root_mapping(
            {
                "environment": environment,
                "database": {"mode": "memory"},
                "dashboard": {"access": {"mode": "mock"}},
            }
        )


@pytest.mark.parametrize("environment", ("development", "test"))
def test_dashboard_allows_mock_access_in_local_environments(environment: str) -> None:
    """@brief Dashboard 仍允许开发/测试 deterministic mock / Dashboard continues to allow deterministic mocks in development/test.

    @param environment 允许 mock operator access 的本地根环境 / Local root environment permitting mock operator access.
    @return 无返回值 / No return value.
    """

    settings = DashboardSettings.from_root_mapping(
        {
            "environment": environment,
            "database": {"mode": "memory"},
            "dashboard": {"access": {"mode": "mock"}},
        }
    )
    assert settings.environment == environment
    assert settings.operator_access_mode == "mock"


def test_dashboard_gui_console_entrypoint_reports_missing_optional_extra(
    monkeypatch: pytest.MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    """@brief GUI console entrypoint 在 headless 安装中应给出受控错误 / GUI console entrypoint must give a controlled error in headless installs.

    @param monkeypatch pytest monkeypatch fixture / pytest monkeypatch fixture.
    @param capsys pytest 标准流捕获夹具 / pytest stdout/stderr capture fixture.
    @return 无返回值 / No return value.
    """

    def missing_gui_extra() -> NoReturn:
        """@brief 模拟未安装的 PyQt6 extra / Simulate a missing PyQt6 extra.

        @return 永不返回 / Never returns.
        """

        raise DashboardDependencyError("Dashboard GUI 需要可选依赖 PyQt6。")

    monkeypatch.setattr(dashboard_gui, "run_gui", missing_gui_extra)
    assert dashboard_gui.main() == 2
    assert "PyQt6" in capsys.readouterr().err


def test_dashboard_gui_console_script_is_declared() -> None:
    """@brief GUI 必须作为单独的可选 console script 发布 / GUI must be published as a separate optional console script.

    @return 无返回值 / No return value.
    """

    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["scripts"]["workspace-dashboard-gui"] == "dashboard.gui:main"
