"""@brief GitHub Actions 供应链引用护栏 / GitHub Actions supply-chain reference guardrails."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final

from conftest import PROJECT_ROOT

_FULL_COMMIT_SHA: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")
"""@brief GitHub Action 不可变完整提交 SHA / Immutable full GitHub Action commit SHA."""

_VERSION_COMMENT: Final[re.Pattern[str]] = re.compile(r"^v[0-9]+(?:\.[0-9]+){1,2}$")
"""@brief 供 Dependabot 与人工审阅使用的版本注释 / Version comment for Dependabot and review."""


def _workflow_paths() -> tuple[Path, ...]:
    """@brief 返回全部 GitHub Actions workflow / Return every GitHub Actions workflow.

    @return 按路径排序的 YAML workflow / YAML workflows sorted by path.
    """

    workflow_root = PROJECT_ROOT / ".github" / "workflows"
    return tuple(sorted((*workflow_root.glob("*.yml"), *workflow_root.glob("*.yaml"))))


def test_external_actions_use_reviewable_immutable_revisions() -> None:
    """@brief 外部 Action 必须固定 SHA 并标注 release / Pin external Actions and label releases.

    @return 无返回值 / No return value.

    @note 浮动 major tag 既可被移动，也可能根本不存在，例如 ``setup-uv@v8``；完整 SHA
    让实际执行代码可审阅，版本注释则保留升级工具所需语义。
    / A floating major tag can move or may not exist at all, as with ``setup-uv@v8``; a full SHA
    makes executed code reviewable while the version comment retains updater semantics.
    """

    workflows = _workflow_paths()
    assert workflows, "仓库必须包含 GitHub Actions workflow"
    external_action_count = 0
    violations: list[str] = []
    for workflow in workflows:
        for line_number, line in enumerate(workflow.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if not stripped.startswith("uses:"):
                continue
            target_and_comment = stripped.removeprefix("uses:").strip()
            target, separator, comment = target_and_comment.partition("#")
            target = target.strip()
            if target.startswith("./"):
                continue
            external_action_count += 1
            if "@" not in target:
                violations.append(f"{workflow}:{line_number}: external action 缺少 revision")
                continue
            _action, revision = target.rsplit("@", 1)
            version = comment.strip() if separator else ""
            if _FULL_COMMIT_SHA.fullmatch(revision) is None:
                violations.append(f"{workflow}:{line_number}: revision 必须是 40 位 commit SHA")
            if _VERSION_COMMENT.fullmatch(version) is None:
                violations.append(f"{workflow}:{line_number}: SHA 后必须标注精确 vX.Y[.Z] release")
    assert external_action_count > 0, "workflow 必须包含至少一个外部 Action"
    assert violations == [], "\n".join(violations)
