"""Regression tests for Docker's single-source runtime configuration."""

from __future__ import annotations

import json
from contextlib import chdir
from pathlib import Path
from typing import Any

import json5
import pytest

from backend.config import BackendSettings
from dashboard.infrastructure.config import DashboardSettings
from dbctl.infrastructure.runtime_projection import validate_runtime_config
from dbctl.interfaces.cli import main as dbctl_main
from workspace_shared.jsonc import ConfigurationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _bootstrap_config(tmp_path: Path) -> Path:
    """Initialize the private config through dbctl with Docker database endpoints."""
    config_path = tmp_path / "config.jsonc"
    with chdir(tmp_path):
        exit_code = dbctl_main(
            [
                "--dbinit",
                str(PROJECT_ROOT / "deploy/docker/dbinit.jsonc"),
                "bootstrap",
                "--dry-run",
            ]
        )
    assert exit_code == 0
    return config_path


def _load_mapping(path: Path) -> dict[str, Any]:
    parsed = json5.loads(path.read_text(encoding="utf-8"))
    assert isinstance(parsed, dict)
    return parsed


def test_container_reads_dbctl_config_without_projection(tmp_path: Path) -> None:
    """The mounted private config remains the exact source read by every service."""
    source_path = _bootstrap_config(tmp_path)
    source = _load_mapping(source_path)
    validate_runtime_config(source_path)

    assert "@postgres:5432/ai_job_workspace" in source["database"]["application_dsn"]
    backend = BackendSettings.from_file(source_path)
    dashboard = DashboardSettings.from_file(source_path)
    assert backend.environment == source["environment"]
    assert backend.network.bind_host == source["network"]["bind_host"]
    assert dashboard.api.host == source["dashboard"]["api"]["host"]


def test_container_entrypoint_requires_dbctl_bootstrap(tmp_path: Path) -> None:
    missing_path = tmp_path / "config.jsonc"
    with pytest.raises(ConfigurationError, match="does not exist"):
        validate_runtime_config(missing_path)
    assert not missing_path.exists()


def test_docker_dbinit_only_changes_connection_endpoint() -> None:
    local = _load_mapping(PROJECT_ROOT / "dbinit.jsonc")
    docker = _load_mapping(PROJECT_ROOT / "deploy/docker/dbinit.jsonc")
    assert docker["database_connection"] == {"host": "postgres", "port": 5432}
    assert docker["database_administration"] == local["database_administration"]


def test_production_config_requires_direct_hmac_secret(tmp_path: Path) -> None:
    source_path = _bootstrap_config(tmp_path)
    root = _load_mapping(source_path)
    root["environment"] = "production"
    root["database"]["mode"] = "postgresql"
    root["security"] = {
        "identity_mode": "trusted_proxy_hmac",
        "trusted_proxy_hmac_secret": None,
        "trusted_proxy_max_clock_skew_seconds": 300,
    }
    source_path.write_text(json.dumps(root), encoding="utf-8")

    with pytest.raises(ConfigurationError, match="trusted_proxy_hmac_secret"):
        BackendSettings.from_file(source_path)
