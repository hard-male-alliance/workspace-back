"""@brief XeLaTeX sandbox 资源边界回归测试 / Regression tests for XeLaTeX sandbox resource boundaries."""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import backend.infrastructure.rendering as rendering
import backend.infrastructure.resume_render_sandbox as render_sandbox
from backend.config import RendererSettings
from backend.domain.common import DomainError
from backend.infrastructure.process_confinement import (
    ProcessConfinementMode,
    ProcessConfinementPlan,
    ProcessConfinementUnavailable,
    clear_confinement_probe_cache,
    confinement_plan_for,
)


def _renderer_settings(tmp_path: Path, *, max_output_bytes: int = 512) -> RendererSettings:
    """@brief 构造测试专用 XeLaTeX 设置 / Build test-only XeLaTeX settings.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param max_output_bytes 编译器输出与 PDF 总上限 / Compiler-output and PDF byte limit.
    @return 受限 renderer 设置 / Restricted renderer settings.
    """

    return RendererSettings(
        adapter="xelatex",
        xelatex_command="xelatex",
        timeout_ms=5_000,
        max_input_bytes=8_192,
        max_output_bytes=max_output_bytes,
        memory_limit_bytes=512 * 1024 * 1024,
        allowed_font_directories=(),
        artifact_directory=tmp_path,
    )


def _resume_document() -> dict[str, Any]:
    """@brief 构造最小合法 SIR / Build a minimal valid SIR.

    @return 仅含 renderer 所需字段的 SIR / SIR containing renderer-required fields only.
    """

    return {
        "id": "res-render-hardening",
        "revision": 1,
        "title": "Klee",
        "profile": {"full_name": "Klee"},
    }


def test_renderer_factory_fails_at_startup_when_sandbox_capability_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 配置真实渲染时在启动期验证能力 / Validate real-rendering capability during startup.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 补丁器 / pytest patch controller.
    """

    monkeypatch.setattr(rendering, "_killpg", lambda _pid, _signal: None)
    monkeypatch.setattr(rendering, "_sigkill", 9)
    monkeypatch.setattr(shutil, "which", lambda _name: "/usr/bin/xelatex")

    def unavailable(_environment: str) -> ProcessConfinementPlan:
        """@brief 模拟 kernel 强隔离不可用 / Simulate unavailable kernel confinement.

        @param _environment 未使用部署环境 / Unused deployment environment.
        @return 不返回 / Does not return.
        @raise ProcessConfinementUnavailable 总是抛出 / Always raised.
        """

        raise ProcessConfinementUnavailable("missing Landlock")

    monkeypatch.setattr(rendering, "confinement_plan_for", unavailable)

    with pytest.raises(RuntimeError, match="strong confinement"):
        rendering.renderer_for(_renderer_settings(tmp_path), environment="production")


def test_sandbox_mounts_only_required_read_only_tex_state_and_private_writable_state(
    tmp_path: Path,
) -> None:
    """@brief TeX 系统状态只读，缓存仅写 sandbox tmpfs / TeX system state is read-only and caches write only to sandbox tmpfs.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    argv = rendering._bubblewrap_argv(
        "/usr/bin/bwrap",
        _renderer_settings(tmp_path),
        tmp_path,
        child_argv=("/usr/bin/python", "-m", "backend.infrastructure.resume_render_sandbox"),
        xelatex_path="/usr/bin/xelatex",
    )
    joined = "\x00".join(argv)

    for required in ("/etc/fonts", "/etc/texmf", "/var/cache/fontconfig", "/var/lib/texmf"):
        assert f"--ro-bind-try\x00{required}\x00{required}" in joined
    assert "--unshare-net" in argv
    assert "--setenv\x00HOME\x00/work" in joined
    assert "--setenv\x00TMPDIR\x00/work/tmp" in joined
    assert "--bind\x00" + str(tmp_path) + "\x00/work" in joined
    assert "backend.infrastructure.resume_render_sandbox" in argv


def test_renderer_landlock_allows_only_the_required_system_paper_definitions() -> None:
    """@brief 允许 XeLaTeX 读取系统纸张规格而不放开整个 `/etc` / Allow only required system paper definitions.

    Debian 的 ``xdvipdfmx`` 通过 ``/etc/paperspecs`` 解析 ``a4`` 等名称；其他
    libpaper 布局使用 ``/etc/papersize``。两者均为只读单文件规则，不能以允许整个
    ``/etc`` 代替。
    """

    paths = render_sandbox._read_only_paths(Path("/usr/bin/xelatex"), ())

    assert Path("/etc/paperspecs") in paths
    assert Path("/etc/papersize") in paths
    assert Path("/etc") not in paths


def test_fixed_template_selects_a_packaged_cjk_capable_font() -> None:
    """@brief 固定模板不会把中文姓名渲染成空白字形 / Fixed template does not render Chinese names as missing glyphs."""

    document = _resume_document()
    document["title"] = "高级工程师"
    document["profile"] = {"full_name": "可莉"}

    source = rendering._safe_template(document)

    assert r"\setmainfont{Noto Sans CJK SC}" in source
    assert "高级工程师" in source
    assert "可莉" in source


def _install_fake_renderer_command(
    monkeypatch: pytest.MonkeyPatch,
    script: str,
    *,
    command_arguments: list[str] | None = None,
) -> None:
    """@brief 将 renderer launcher 替换为受控 Python 子进程 / Replace the renderer launcher with a controlled Python subprocess.

    @param monkeypatch pytest 补丁器 / pytest patch controller.
    @param script 要在隔离 session 中执行的 Python 源码 / Python source to run in the isolated session.
    @param command_arguments 传给脚本的额外参数 / Extra arguments supplied to the script.
    @return 无返回值 / No return value.
    """

    arguments = command_arguments or []
    monkeypatch.setattr(
        rendering,
        "_renderer_process_argv",
        lambda _xelatex, _settings, _workdir, _plan: [
            sys.executable,
            "-c",
            script,
            *arguments,
        ],
    )


async def _wait_for_file(path: Path) -> None:
    """@brief 等待测试子进程创建同步文件 / Wait for a test subprocess synchronization file.

    @param path 预期出现的文件 / File expected to appear.
    @return 无返回值 / No return value.
    @raise AssertionError 限时内未出现时抛出 / Raised when the file does not appear before the deadline.
    """

    for _ in range(100):
        if await asyncio.to_thread(path.is_file):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"test subprocess did not create {path}")


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group semantics are required")
@pytest.mark.asyncio
async def test_renderer_kills_the_whole_process_group_when_combined_pipe_output_exceeds_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 合并 stdout/stderr 超限后，不留下脱离父进程的子进程 / Leave no detached child after combined stdout/stderr exceeds the limit.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 补丁器 / pytest patch controller.
    @return 无返回值 / No return value.
    """

    marker = tmp_path / "survived-output-limit"
    child_script = (
        "import pathlib, time\n"
        "time.sleep(0.25)\n"
        f"pathlib.Path({str(marker)!r}).write_text('survived', encoding='utf-8')\n"
    )
    script = (
        "import signal, subprocess, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"subprocess.Popen([sys.executable, '-c', {child_script!r}])\n"
        "sys.stdout.write('x' * 4_194_304)\n"
        "sys.stdout.flush()\n"
        "time.sleep(30)\n"
    )
    _install_fake_renderer_command(monkeypatch, script)

    renderer = rendering.SandboxedXeLaTeXRenderer(
        _renderer_settings(tmp_path, max_output_bytes=128),
        xelatex_path=sys.executable,
    )
    with pytest.raises(DomainError) as raised:
        await renderer.render(_resume_document())

    assert raised.value.problem.code == "resume.render_output_too_large"
    await asyncio.sleep(0.4)
    assert not marker.exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group semantics are required")
@pytest.mark.asyncio
async def test_renderer_cancellation_kills_the_whole_process_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 取消 render coroutine 后，不留下编译器后代 / Leave no compiler descendant after cancelling the render coroutine.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 补丁器 / pytest patch controller.
    @return 无返回值 / No return value.
    """

    ready = tmp_path / "compiler-ready"
    marker = tmp_path / "survived-cancellation"
    child_script = (
        "import pathlib, time\n"
        "time.sleep(0.25)\n"
        f"pathlib.Path({str(marker)!r}).write_text('survived', encoding='utf-8')\n"
    )
    script = (
        "import pathlib, subprocess, sys, time\n"
        f"pathlib.Path({str(ready)!r}).write_text('ready', encoding='utf-8')\n"
        f"subprocess.Popen([sys.executable, '-c', {child_script!r}])\n"
        "time.sleep(30)\n"
    )
    _install_fake_renderer_command(monkeypatch, script)

    renderer = rendering.SandboxedXeLaTeXRenderer(
        _renderer_settings(tmp_path),
        xelatex_path=sys.executable,
    )
    task = asyncio.create_task(renderer.render(_resume_document()))
    await _wait_for_file(ready)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(0.4)
    assert not marker.exists()


def test_limited_pdf_rejects_oversized_file_before_opening_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief 超大 PDF 在 open/read 之前由 stat 拒绝 / Reject oversized PDF from stat before open/read.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param monkeypatch pytest 补丁器 / pytest patch controller.
    @return 无返回值 / No return value.
    """

    artifact = tmp_path / "oversized.pdf"
    artifact.write_bytes(b"%PDF-" + b"x" * 128)

    def unexpected_open(_path: Path, *_args: object, **_kwargs: object) -> object:
        """@brief 若代码尝试读取超大文件则使测试失败 / Fail the test if code tries to read an oversized file.

        @param _path 被打开的路径 / Path that would be opened.
        @param _args 位置参数 / Positional arguments.
        @param _kwargs 关键字参数 / Keyword arguments.
        @return 不返回 / Does not return.
        @raise AssertionError 总是抛出 / Always raised.
        """

        raise AssertionError("oversized PDF must be rejected before open")

    monkeypatch.setattr(Path, "open", unexpected_open)
    with pytest.raises(DomainError) as raised:
        rendering._read_limited_pdf(artifact, 64)

    assert raised.value.problem.code == "resume.invalid_pdf"


@pytest.mark.skipif(os.name == "posix", reason="Non-POSIX fail-closed behavior only")
@pytest.mark.asyncio
async def test_renderer_fails_closed_without_a_posix_sandbox(tmp_path: Path) -> None:
    """@brief 非 POSIX 平台明确拒绝真实渲染 / Fail closed on non-POSIX platforms.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """

    renderer = rendering.SandboxedXeLaTeXRenderer(_renderer_settings(tmp_path))
    with pytest.raises(DomainError) as raised:
        await renderer.render(_resume_document())

    assert raised.value.problem.code == "resume.renderer_sandbox_unavailable"


def test_minimal_xelatex_document_compiles_through_real_strong_boundary(
    tmp_path: Path,
) -> None:
    """@brief 最小 TeX 经真实 Landlock/libseccomp launcher 编译 / Compile minimal TeX through the real strong launcher.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    xelatex = shutil.which("xelatex")
    if xelatex is None:
        pytest.skip("XeLaTeX is unavailable")
    clear_confinement_probe_cache()
    try:
        probed_plan = confinement_plan_for("production")
    except ProcessConfinementUnavailable:
        pytest.skip("Landlock ABI >= 3 and libseccomp are unavailable")
    plan = ProcessConfinementPlan(ProcessConfinementMode.STRONG, None)
    assert probed_plan.mode is ProcessConfinementMode.STRONG
    settings = _renderer_settings(tmp_path, max_output_bytes=4 * 1024 * 1024)
    (tmp_path / "resume.tex").write_text(
        "\\documentclass{article}\n"
        "\\pagestyle{empty}\n"
        "\\begin{document}\n"
        "Confinement boundary smoke test.\n"
        "\\end{document}\n",
        encoding="utf-8",
    )
    argv = rendering._renderer_process_argv(xelatex, settings, tmp_path, plan)

    completed = subprocess.run(
        argv,
        cwd=tmp_path,
        env=rendering._renderer_process_environment(tmp_path),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=15,
        start_new_session=True,
    )

    diagnostic = (completed.stdout + completed.stderr).decode("utf-8", "replace")
    assert completed.returncode == 0, diagnostic[-4_000:]
    assert (tmp_path / "resume.pdf").read_bytes().startswith(b"%PDF-")
