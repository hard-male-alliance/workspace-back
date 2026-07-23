"""@brief Linux 子进程强隔离 capability 回归 / Linux child-process strong-confinement regressions."""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.infrastructure import process_confinement
from backend.infrastructure.process_confinement import (
    ProcessConfinementMode,
    ProcessConfinementUnavailable,
)


def test_production_fails_closed_when_strong_confinement_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 生产环境缺少 Landlock/libseccomp 时启动失败 / Production startup fails without Landlock/libseccomp.

    @param monkeypatch pytest 补丁器 / pytest patch controller.
    """

    monkeypatch.setattr(process_confinement, "_strong_confinement_probe", lambda: False)

    with pytest.raises(ProcessConfinementUnavailable, match="Landlock and libseccomp"):
        process_confinement.confinement_plan_for("production")


def test_production_does_not_require_optional_bubblewrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 强隔离可在没有 Bubblewrap 时成立 / Strong confinement does not require Bubblewrap.

    @param monkeypatch pytest 补丁器 / pytest patch controller.
    """

    monkeypatch.setattr(process_confinement, "_strong_confinement_probe", lambda: True)
    monkeypatch.setattr(
        "backend.infrastructure.process_confinement.shutil.which",
        lambda _name: None,
    )

    plan = process_confinement.confinement_plan_for("production")

    assert plan.mode is ProcessConfinementMode.STRONG
    assert plan.bubblewrap is None


def test_unusable_bubblewrap_is_not_selected_as_an_extra_layer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief Bubblewrap 必须真实 probe 成功才可叠加 / Bubblewrap is selected only after a real probe.

    @param monkeypatch pytest 补丁器 / pytest patch controller.
    """

    monkeypatch.setattr(process_confinement, "_strong_confinement_probe", lambda: True)
    monkeypatch.setattr(
        "backend.infrastructure.process_confinement.shutil.which",
        lambda _name: "/usr/bin/bwrap",
    )
    monkeypatch.setattr(process_confinement, "_bubblewrap_probe", lambda _binary: False)

    plan = process_confinement.confinement_plan_for("production")

    assert plan.mode is ProcessConfinementMode.STRONG
    assert plan.bubblewrap is None


def test_real_production_probe_verifies_landlock_and_libseccomp() -> None:
    """@brief 真实 child probe 同时验证文件与 syscall 拒绝 / Real child probe verifies filesystem and syscall denials."""

    process_confinement.clear_confinement_probe_cache()
    try:
        plan = process_confinement.confinement_plan_for("production")
    except ProcessConfinementUnavailable:
        pytest.skip("Landlock ABI >= 3 and libseccomp are unavailable")

    assert plan.mode is ProcessConfinementMode.STRONG
    assert plan.bubblewrap is None or Path(plan.bubblewrap).is_absolute()
