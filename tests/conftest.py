"""@brief 后端纵向切片测试的共享夹具 / Shared fixtures for backend vertical-slice tests."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import json5
import pytest
from fastapi.testclient import TestClient

from backend.app import create_app
from backend.config import BackendSettings
from backend.infrastructure.contracts import ContractValidator
from dbctl.infrastructure.configuration import DbctlConfigStore

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Repository root directory."""

CONTRACT_DIRECTORY = PROJECT_ROOT / "workspace-shared-docs" / "contracts" / "v1"
"""@brief 共享契约 submodule 中的 v1 目录 / V1 directory in the shared-contract submodule."""

CONTRACT_SCHEMA_PATH = CONTRACT_DIRECTORY / "ai-job-workspace.contract.schema.json"
"""@brief 严格 JSON Schema 的权威路径 / Authoritative strict JSON Schema path."""

CONTRACT_SCHEMA_JSONC_PATH = CONTRACT_DIRECTORY / "ai-job-workspace.contract.schema.jsonc"
"""@brief 带注释 Schema 源文件路径 / Comment-bearing schema source path."""

CONTRACT_EXAMPLES_PATH = CONTRACT_DIRECTORY / "ai-job-workspace.contract.examples.jsonc"
"""@brief 已发布 JSONC 示例路径 / Published JSONC examples path."""


@pytest.fixture
def dbctl_config_path(tmp_path: Path) -> Path:
    """@brief 初始化隔离的 dbctl 私密配置 / Initialize an isolated private dbctl config.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    @return 含随机测试凭证的 config.jsonc 路径 / config.jsonc path with random test credentials.
    """
    config_path = tmp_path / "config.jsonc"
    config_path.write_text((PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8"))
    DbctlConfigStore(config_path, PROJECT_ROOT / "dbinit.jsonc").initialize()
    return config_path


@pytest.fixture
def contract_bundle() -> dict[str, Any]:
    """@brief 加载严格的正式契约 Bundle / Load the strict formal contract bundle.

    @return 已解析的 JSON Schema 对象 / Parsed JSON Schema object.
    """

    payload = json.loads(CONTRACT_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


@pytest.fixture
def contract_examples() -> dict[str, Any]:
    """@brief 加载正式 JSONC 示例 / Load the official JSONC examples.

    @return 以示例名称为键的请求对象 / Request objects keyed by example name.
    """

    payload = json5.loads(CONTRACT_EXAMPLES_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


@pytest.fixture
def contract_validator() -> ContractValidator:
    """@brief 创建直接使用权威 Schema 的验证器 / Create a validator using the authoritative schema.

    @return ContractValidator 实例 / ContractValidator instance.
    """

    return ContractValidator(CONTRACT_SCHEMA_PATH)


@pytest.fixture
def backend_client() -> Iterator[TestClient]:
    """@brief 启动带 memory/mock adapter 的完整 FastAPI 生命周期 / Start full FastAPI lifecycle with memory/mock adapters.

    @return 仍处于 lifespan 中的 TestClient / TestClient still inside lifespan.
    @note 必须使用上下文管理器，以便后台 supervisor 与 telemetry 被可靠关闭。
    """

    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")
    settings = replace(
        settings,
        api=replace(settings.api, legacy_v1_enabled=True),
        network=replace(
            settings.network,
            cors_allowed_origins=("http://127.0.0.1:5173", "http://localhost:5173"),
        ),
    )
    application = create_app(settings)
    with TestClient(application, raise_server_exceptions=False) as client:
        yield client


def idempotency_headers(key: str) -> dict[str, str]:
    """@brief 构建命令请求需要的幂等头 / Build required idempotency headers for commands.

    @param key 稳定、非空的重试键 / Stable non-empty retry key.
    @return HTTP header 字典 / HTTP header dictionary.
    """

    return {"Idempotency-Key": key}


def wait_for_json(
    client: TestClient,
    path: str,
    completed: Callable[[dict[str, Any]], bool],
    *,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    """@brief 轮询受控后台 Job，直到其公开状态满足条件 / Poll a bounded background job until public state matches.

    @param client 已启动的 TestClient / Started TestClient.
    @param path 待轮询路径 / Path to poll.
    @param completed 判断已完成状态的谓词 / Predicate for a completed payload.
    @param timeout_seconds 最长等待时间 / Maximum wait time in seconds.
    @return 最后满足谓词的 JSON 对象 / JSON object satisfying the predicate.
    @raise AssertionError 接口失败或超时时抛出 / Raised for an endpoint failure or timeout.
    """

    deadline = time.monotonic() + timeout_seconds
    last_payload: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        response = client.get(path)
        assert response.status_code == 200, response.text
        payload = response.json()
        assert isinstance(payload, dict)
        last_payload = payload
        if completed(payload):
            return payload
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {path}: {last_payload!r}")
