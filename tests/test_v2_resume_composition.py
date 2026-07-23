"""@brief Resume V2 composition root 回归 / Resume V2 composition-root regression."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from backend.composition import build_container
from backend.config import BackendSettings

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根目录 / Repository root."""

_RESUME_APPLICATION_OPERATIONS = (
    "list_resumes",
    "create_resume",
    "get_resume",
    "update_resume_metadata",
    "delete_resume",
    "list_revisions",
    "get_revision",
    "apply_operations",
    "create_import_job",
    "create_restore_job",
    "create_render_job",
    "list_proposals",
    "get_proposal",
    "decide_proposal",
)
"""@brief API 5.2 需要的应用操作 / Application operations required by API section 5.2."""


@pytest.mark.asyncio
async def test_memory_composition_exposes_resume_use_cases_without_a_fake_durable_worker(
    tmp_path: Path,
) -> None:
    """@brief memory 组合提供同步领域测试面但不伪造 durable worker / Memory composition exposes synchronous domain tests without fabricating a durable worker.

    @param tmp_path pytest 私有运行根 / Pytest-private runtime root.
    """

    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")

    async with build_container(settings, tmp_path) as container:
        assert all(
            callable(getattr(container.resumes_v2, operation))
            for operation in _RESUME_APPLICATION_OPERATIONS
        )
        assert "aiws:resume:v2-outbox" not in {
            task.get_name() for task in asyncio.all_tasks()
        }
