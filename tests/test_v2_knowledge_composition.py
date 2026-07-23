"""@brief Knowledge V2 composition 与本地签名上传边界 / Knowledge V2 composition and local signed-upload boundary."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app import create_app
from backend.application.knowledge import V2_KNOWLEDGE_ENDPOINT_METHODS
from backend.composition import build_container
from backend.config import BackendSettings, KnowledgeLocalUploadStorageSettings

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根目录 / Repository root."""


def _settings(tmp_path: Path) -> BackendSettings:
    """@brief 将 development local store 定向测试目录 / Point the development local store at the test directory.

    @param tmp_path pytest 私有目录 / Pytest-private directory.
    @return 不含持久 secret 的 development 配置 / Development settings without durable secrets.
    """

    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")
    storage = settings.knowledge.uploads.storage
    assert isinstance(storage, KnowledgeLocalUploadStorageSettings)
    local = replace(storage, directory=Path("knowledge-uploads"))
    knowledge = replace(
        settings.knowledge,
        uploads=replace(settings.knowledge.uploads, storage=local),
    )
    return replace(
        settings,
        knowledge=knowledge,
        config_path=tmp_path / "config.jsonc",
    )


@pytest.mark.asyncio
async def test_memory_container_exposes_every_knowledge_v2_operation(tmp_path: Path) -> None:
    """@brief memory composition 显式提供 5.3 全部用例 / Memory composition exposes every section 5.3 use case."""

    settings = _settings(tmp_path)
    async with build_container(settings, tmp_path) as container:
        assert all(
            callable(getattr(container.knowledge_v2, method))
            for method in V2_KNOWLEDGE_ENDPOINT_METHODS
        )
        assert container.knowledge_local_upload_store is not None
        assert (tmp_path / "knowledge-uploads").is_dir()


def test_local_upload_route_is_authenticated_by_grant_not_bearer(tmp_path: Path) -> None:
    """@brief local PUT 到达签名验证而不被 Bearer middleware 误拦 / Local PUT reaches grant verification rather than Bearer middleware."""

    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        response = client.put(
            "/__local-uploads/workspace_localtest1/upload_localtest01",
            params={
                "expires": "4102444800",
                "size": "1",
                "sha256": "0" * 64,
                "signature": "invalid",
            },
            headers={
                "Content-Length": "1",
                "X-AIWS-Content-SHA256": "0" * 64,
            },
            content=b"x",
        )

    assert response.status_code == 403
    assert response.json()["code"] == "http.request_rejected"
    assert response.json()["instance"].startswith("/__local-uploads/")
    assert "WWW-Authenticate" not in response.headers
