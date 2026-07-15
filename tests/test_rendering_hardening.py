"""@brief XeLaTeX sandbox 资源边界回归测试 / Regression tests for XeLaTeX sandbox resource boundaries."""

from __future__ import annotations

import asyncio
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest

import backend.infrastructure.rendering as rendering
from backend.config import RendererSettings
from backend.domain.common import DomainError


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
        memory_limit_bytes=256 * 1024 * 1024,
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


def _install_fake_sandbox_command(
    monkeypatch: pytest.MonkeyPatch,
    script: str,
    *,
    command_arguments: list[str] | None = None,
) -> None:
    """@brief 将 bwrap argv 替换为受控 Python 子进程 / Replace bwrap argv with a controlled Python subprocess.

    @param monkeypatch pytest 补丁器 / pytest patch controller.
    @param script 要在隔离 session 中执行的 Python 源码 / Python source to run in the isolated session.
    @param command_arguments 传给脚本的额外参数 / Extra arguments supplied to the script.
    @return 无返回值 / No return value.
    """

    arguments = command_arguments or []
    monkeypatch.setattr(shutil, "which", lambda _name: "/test/bwrap")
    monkeypatch.setattr(
        rendering,
        "_bubblewrap_argv",
        lambda _bubblewrap, _settings, _workdir: [sys.executable, "-c", script, *arguments],
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
    _install_fake_sandbox_command(monkeypatch, script)

    renderer = rendering.SandboxedXeLaTeXRenderer(_renderer_settings(tmp_path, max_output_bytes=128))
    with pytest.raises(DomainError) as raised:
        await renderer.render(_resume_document())

    assert raised.value.problem.code == "resume.render_output_too_large"
    await asyncio.sleep(0.4)
    assert not marker.exists()


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
    _install_fake_sandbox_command(monkeypatch, script)

    renderer = rendering.SandboxedXeLaTeXRenderer(_renderer_settings(tmp_path))
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
