"""@brief 受限简历渲染基础设施 / Restricted resume-rendering infrastructure."""

from __future__ import annotations

import asyncio
import importlib
import os
import shutil
import signal
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from backend.config import RendererSettings
from backend.domain.common import DomainError, Problem
from workspace_shared.ids import new_opaque_id

_resource: Any = importlib.import_module("resource") if os.name == "posix" else None
"""@brief POSIX resource 模块；非 POSIX 平台保持为空 / POSIX resource module, absent elsewhere."""

_killpg: Callable[[int, int], None] | None = getattr(os, "killpg", None)
"""@brief POSIX 进程组信号函数 / POSIX process-group signal function."""

_sigkill: int | None = getattr(signal, "SIGKILL", None)
"""@brief POSIX 强制终止信号 / POSIX force-termination signal."""

_PIPE_READ_CHUNK_BYTES = 16 * 1024
"""@brief 单次诊断 pipe 读取上限 / Maximum bytes read from one diagnostic pipe at a time."""

_DIAGNOSTIC_CAPTURE_BYTES = 4 * 1024
"""@brief 保存在内存中的诊断前缀上限 / Maximum diagnostic prefix retained in memory."""

_PROCESS_TERMINATION_GRACE_SECONDS = 1.0
"""@brief 给 SIGTERM 的清理宽限期 / Cleanup grace period granted to SIGTERM."""

_PROCESS_KILL_REAP_SECONDS = 1.0
"""@brief 发出 SIGKILL 后等待回收的上限 / Maximum wait for reaping after SIGKILL."""


class _CombinedOutputLimitExceeded(Exception):
    """@brief 编译器合并输出超过配额 / Compiler combined output exceeded its budget."""


@dataclass(slots=True)
class _BoundedCompilerOutput:
    """@brief 有界收集编译器 pipe 输出 / Collect compiler-pipe output with strict bounds.

    @note ``bytes_seen`` 始终计入 stdout 与 stderr；只保留有限 stderr 前缀，避免把
    非结构化诊断变成进程内存的无界输入。
    """

    max_output_bytes: int
    bytes_seen: int = 0
    stderr_prefix: bytearray = field(default_factory=bytearray)

    def consume(self, chunk: bytes, *, is_stderr: bool) -> None:
        """@brief 计入一段 pipe 输出 / Account for one chunk of pipe output.

        @param chunk 新读取的输出字节 / Newly read output bytes.
        @param is_stderr 是否来自标准错误 / Whether the bytes came from stderr.
        @raise _CombinedOutputLimitExceeded 合并输出超过硬上限时抛出 / Raised when combined output crosses the hard limit.
        """

        self.bytes_seen += len(chunk)
        if is_stderr and len(self.stderr_prefix) < _DIAGNOSTIC_CAPTURE_BYTES:
            remaining = _DIAGNOSTIC_CAPTURE_BYTES - len(self.stderr_prefix)
            self.stderr_prefix.extend(chunk[:remaining])
        if self.bytes_seen > self.max_output_bytes:
            raise _CombinedOutputLimitExceeded


class MockRenderer:
    """@brief 确定性 PDF renderer mock / Deterministic PDF renderer mock.

    @note MOCK — 只用于测试和无 TeX 的本地研发；不执行用户提供的 LaTeX。
    """

    async def render(self, document: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
        """@brief 生成最小 PDF 与 source map / Produce a minimal PDF and source map.

        @param document ResumeDocument SIR / ResumeDocument SIR.
        @return PDF bytes and a semantic source map / PDF 字节与语义 source map.
        """
        await asyncio.sleep(0)
        pdf = _minimal_pdf(document["title"])
        artifact_id = new_opaque_id("art")
        nodes = [
            {
                "node_kind": "section",
                "node_id": section["section_id"],
                "field_path": [],
                "page": 1,
                "rects": [{"x": 36.0, "y": 760.0 - index * 36.0, "width": 520.0, "height": 24.0, "unit": "pt"}],
            }
            for index, section in enumerate(document["sections"])
        ]
        source_map = {
            "schema_version": "1.0",
            "resume_id": document["id"],
            "resume_revision": document["revision"],
            "artifact_id": artifact_id,
            "page_count": 1,
            "nodes": nodes,
        }
        return pdf, source_map


class SandboxedXeLaTeXRenderer:
    """@brief 仅在 OS sandbox 内运行的 XeLaTeX renderer / XeLaTeX renderer that runs only inside an OS sandbox.

    @note 若 bubblewrap 不可用则 fail closed；`-no-shell-escape` 自身不是安全边界。
    """

    def __init__(self, settings: RendererSettings) -> None:
        """@brief 初始化受限 renderer / Initialize the restricted renderer.

        @param settings 编译约束 / Compilation constraints.
        """
        self._settings = settings

    async def render(self, document: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
        """@brief 在隔离进程中编译固定模板 / Compile a fixed template in an isolated process.

        @param document 已验证的 SIR / Validated SIR.
        @return PDF bytes and semantic source map / PDF 字节与语义 source map.
        @raise DomainError sandbox 缺失、超时或编译失败时抛出 / Raised for a missing sandbox, timeout, or compilation failure.
        """
        if _resource is None or _killpg is None or _sigkill is None:
            raise DomainError(
                Problem(
                    "resume.renderer_sandbox_unavailable",
                    503,
                    "Secure XeLaTeX sandbox is unavailable",
                    detail="Real XeLaTeX rendering requires a POSIX process sandbox.",
                )
            )
        bubblewrap = shutil.which("bwrap")
        if bubblewrap is None:
            raise DomainError(
                Problem(
                    "resume.renderer_sandbox_unavailable",
                    503,
                    "Secure XeLaTeX sandbox is unavailable",
                    detail="Real XeLaTeX rendering is disabled until an OS sandbox is installed.",
                )
            )
        latex = _safe_template(document)
        if len(latex.encode("utf-8")) > self._settings.max_input_bytes:
            raise DomainError(Problem("resume.input_too_large", 413, "Resume render input is too large"))
        with tempfile.TemporaryDirectory(prefix="aiws-xelatex-") as temporary_directory:
            workdir = Path(temporary_directory)
            source_path = workdir / "resume.tex"
            source_path.write_text(latex, encoding="utf-8")
            argv = _bubblewrap_argv(bubblewrap, self._settings, workdir)
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=workdir,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
                preexec_fn=_resource_limiter(self._settings),
            )
            process_group_cleaned = False
            try:
                try:
                    stderr = await asyncio.wait_for(
                        _collect_bounded_process_output(process, self._settings.max_output_bytes),
                        self._settings.timeout_ms / 1000,
                    )
                except TimeoutError as error:
                    raise DomainError(
                        Problem(
                            "resume.render_timeout",
                            504,
                            "Resume compilation timed out",
                            retryable=True,
                        )
                    ) from error
                except _CombinedOutputLimitExceeded as error:
                    await _terminate_process_group(process, force=True)
                    process_group_cleaned = True
                    raise DomainError(
                        Problem(
                            "resume.render_output_too_large",
                            422,
                            "Renderer diagnostic output is too large",
                        )
                    ) from error
                output_path = workdir / "resume.pdf"
                if process.returncode != 0 or not output_path.is_file():
                    raise DomainError(
                        Problem(
                            "resume.render_failed",
                            422,
                            "Resume compilation failed",
                            extensions={"exit_code": process.returncode, "diagnostic": _safe_diagnostic(stderr)},
                        )
                    )
                content = _read_limited_pdf(output_path, self._settings.max_output_bytes)
                if not content.startswith(b"%PDF-"):
                    raise DomainError(Problem("resume.invalid_pdf", 422, "Renderer did not produce an acceptable PDF"))
            finally:
                if not process_group_cleaned:
                    await _terminate_process_group(process)
        artifact_id = new_opaque_id("art")
        source_map = {
            "schema_version": "1.0",
            "resume_id": document["id"],
            "resume_revision": document["revision"],
            "artifact_id": artifact_id,
            "page_count": 1,
            "nodes": [],
        }
        return content, source_map


def renderer_for(settings: RendererSettings) -> MockRenderer | SandboxedXeLaTeXRenderer:
    """@brief 按配置选择 renderer / Select a renderer from configuration.

    @param settings 渲染设置 / Renderer settings.
    @return 私有 renderer 实现 / Private renderer implementation.
    """
    return MockRenderer() if settings.adapter == "mock" else SandboxedXeLaTeXRenderer(settings)


def _minimal_pdf(title: object) -> bytes:
    """@brief 创建确定性的最小 PDF / Create a deterministic minimal PDF.

    @param title 未信任标题 / Untrusted title.
    @return 最小 PDF 字节 / Minimal PDF bytes.
    """
    safe_title = str(title).encode("ascii", "replace")[:80]
    content = b"BT /F1 12 Tf 72 720 Td (" + safe_title.replace(b"(", b"[").replace(b")", b"]") + b") Tj ET"
    objects = [
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n",
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n",
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj\n",
        b"4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n",
        b"5 0 obj << /Length " + str(len(content)).encode() + b" >> stream\n" + content + b"\nendstream endobj\n",
    ]
    output = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for item in objects:
        offsets.append(len(output))
        output.extend(item)
    xref_offset = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode())
    output.extend(b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets[1:]))
    output.extend(f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode())
    return bytes(output)


def _safe_template(document: dict[str, Any]) -> str:
    """@brief 从 SIR 生成固定受限 TeX 模板 / Generate a fixed restricted TeX template from SIR.

    @param document 已验证 SIR / Validated SIR.
    @return 无用户 TeX 命令的固定模板 / Fixed template containing no user TeX commands.
    """
    title = _latex_escape(str(document.get("title", "")))
    full_name = _latex_escape(str(document.get("profile", {}).get("full_name", "")))
    return "\\documentclass[10pt]{article}\n\\usepackage{fontspec}\n\\pagestyle{empty}\n\\begin{document}\n\\section*{" + title + "}\n" + full_name + "\n\\end{document}\n"


def _latex_escape(value: str) -> str:
    """@brief 转义用户文本而非解释为 TeX / Escape user text instead of interpreting it as TeX.

    @param value 未信任文本 / Untrusted text.
    @return 转义结果 / Escaped result.
    """
    replacements = {"\\": "\\textbackslash{}", "{": "\\{", "}": "\\}", "#": "\\#", "$": "\\$", "%": "\\%", "&": "\\&", "_": "\\_", "^": "\\textasciicircum{}", "~": "\\textasciitilde{}"}
    return "".join(replacements.get(character, character) for character in value)


def _bubblewrap_argv(bubblewrap: str, settings: RendererSettings, workdir: Path) -> list[str]:
    """@brief 生成固定 bubblewrap 参数 / Build fixed bubblewrap arguments.

    @param bubblewrap bwrap 可执行路径 / bwrap executable path.
    @param settings 渲染限制 / Render limits.
    @param workdir 临时工作目录 / Temporary work directory.
    @return 不含 shell 的 argv / Shell-free argv.
    """
    argv = [
        bubblewrap,
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--unshare-pid",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--tmpfs",
        "/tmp",
        "--ro-bind",
        "/usr",
        "/usr",
        "--ro-bind-try",
        "/lib",
        "/lib",
        "--ro-bind-try",
        "/lib64",
        "/lib64",
        "--bind",
        str(workdir),
        "/work",
        "--chdir",
        "/work",
    ]
    for font_directory in settings.allowed_font_directories:
        source = Path(font_directory)
        if source.is_dir():
            argv.extend(["--ro-bind", str(source.resolve()), str(source.resolve())])
    argv.extend(
        [
            settings.xelatex_command,
            "-no-shell-escape",
            "-halt-on-error",
            "-file-line-error",
            "-interaction=nonstopmode",
            "-output-directory=/work",
            "resume.tex",
        ]
    )
    return argv


def _resource_limiter(settings: RendererSettings) -> Any:
    """@brief 返回子进程资源限制器 / Return a child-process resource limiter.

    @param settings 渲染限制 / Render limits.
    @return POSIX pre-exec callable / POSIX pre-exec callable.
    """
    if _resource is None:
        raise RuntimeError("POSIX resource limits are unavailable on this platform")

    def limit_resources() -> None:
        """@brief 在 child 中设置 rlimit / Set rlimits in the child."""
        _resource.setrlimit(_resource.RLIMIT_AS, (settings.memory_limit_bytes, settings.memory_limit_bytes))
        _resource.setrlimit(_resource.RLIMIT_FSIZE, (settings.max_output_bytes, settings.max_output_bytes))
        _resource.setrlimit(_resource.RLIMIT_NPROC, (32, 32))

    return limit_resources


async def _collect_bounded_process_output(
    process: asyncio.subprocess.Process,
    max_output_bytes: int,
) -> bytes:
    """@brief 并行、有界地排空编译器诊断 pipe / Concurrently drain compiler diagnostics with a hard bound.

    @param process 已启动的隔离编译进程 / Started isolated compiler process.
    @param max_output_bytes stdout 与 stderr 合并字节上限 / Combined stdout and stderr byte limit.
    @return 已截断保留的 stderr 前缀 / Retained bounded stderr prefix.
    @raise _CombinedOutputLimitExceeded 合并输出超过硬上限时抛出 / Raised when combined output crosses the hard limit.
    @note 本函数本身不发送信号；调用方必须在 ``finally`` 中终止进程组，以便取消、超时
    与输出超限共享同一条清理路径。
    """

    collector = _BoundedCompilerOutput(max_output_bytes=max_output_bytes)
    stdout = _require_process_pipe(process.stdout, "stdout")
    stderr = _require_process_pipe(process.stderr, "stderr")
    tasks = {
        asyncio.create_task(_drain_compiler_pipe(stdout, collector, is_stderr=False)),
        asyncio.create_task(_drain_compiler_pipe(stderr, collector, is_stderr=True)),
        asyncio.create_task(_wait_for_process_exit(process)),
    }
    try:
        while tasks:
            completed, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            first_error: BaseException | None = None
            for task in completed:
                try:
                    task.result()
                except BaseException as error:
                    if first_error is None:
                        first_error = error
            if first_error is not None:
                raise first_error
        return bytes(collector.stderr_prefix)
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def _drain_compiler_pipe(
    pipe: asyncio.StreamReader,
    collector: _BoundedCompilerOutput,
    *,
    is_stderr: bool,
) -> None:
    """@brief 逐块排空一个编译器 pipe / Drain one compiler pipe incrementally.

    @param pipe 待读取的 stdout 或 stderr / stdout or stderr stream to consume.
    @param collector 合并输出计数器 / Combined-output collector.
    @param is_stderr 是否保存该 pipe 的安全诊断前缀 / Whether to retain a safe diagnostic prefix.
    @return 无返回值 / No return value.
    """

    while chunk := await pipe.read(_PIPE_READ_CHUNK_BYTES):
        collector.consume(chunk, is_stderr=is_stderr)


async def _wait_for_process_exit(process: asyncio.subprocess.Process) -> None:
    """@brief 等待编译进程退出 / Wait for the compiler process to exit.

    @param process 编译子进程 / Compiler subprocess.
    @return 无返回值 / No return value.
    """

    await process.wait()


def _require_process_pipe(
    pipe: asyncio.StreamReader | None,
    name: str,
) -> asyncio.StreamReader:
    """@brief 断言 subprocess 已配置诊断 pipe / Assert that a subprocess diagnostic pipe exists.

    @param pipe asyncio 提供的 pipe / Pipe supplied by asyncio.
    @param name pipe 名称 / Pipe name.
    @return 可读取 pipe / Readable pipe.
    @raise RuntimeError 进程创建约束被破坏时抛出 / Raised when the subprocess setup invariant is broken.
    """

    if pipe is None:
        raise RuntimeError(f"compiler {name} pipe was not configured")
    return pipe


def _read_limited_pdf(path: Path, max_output_bytes: int) -> bytes:
    """@brief 在读入前检查并有界读取 PDF / Stat-check and bounded-read a PDF before loading it.

    @param path sandbox 生成的 PDF 路径 / PDF path produced by the sandbox.
    @param max_output_bytes PDF 最大允许字节数 / Maximum permitted PDF bytes.
    @return 已读取的 PDF 字节 / Read PDF bytes.
    @raise DomainError 文件尺寸超过配额时抛出 / Raised when the file exceeds its byte budget.
    @note ``stat`` 是第一道拒绝路径；随后显式指定 ``read`` 上限以防止文件在检查和读取
    之间被意外增长时造成无界内存分配。
    """

    if path.stat().st_size > max_output_bytes:
        raise DomainError(Problem("resume.invalid_pdf", 422, "Renderer did not produce an acceptable PDF"))
    with path.open("rb") as pdf_file:
        content = pdf_file.read(max_output_bytes + 1)
    if len(content) > max_output_bytes:
        raise DomainError(Problem("resume.invalid_pdf", 422, "Renderer did not produce an acceptable PDF"))
    return content


async def _terminate_process_group(
    process: asyncio.subprocess.Process,
    *,
    force: bool = False,
) -> None:
    """@brief 终止整个编译进程组 / Terminate the entire compiler process group.

    @param process 编译子进程 / Compiler subprocess.
    @param force 是否直接使用 SIGKILL / Whether to use SIGKILL immediately.
    @note 即使 session leader 已退出，仍检查并终止其同组后代；清理期间继续丢弃 pipe
    内容，以避免 ``Process.wait`` 因满 pipe 而卡住。输出配额违规使用 ``force``，因为
    不可信进程可能忽略 SIGTERM 并继续写入。
    """

    if _sigkill is None:
        raise RuntimeError("POSIX process-group termination is unavailable on this platform")

    process_group_id = process.pid
    if force:
        _signal_process_group(process_group_id, _sigkill)
        await _wait_and_discard_process_output(
            process,
            timeout_seconds=_PROCESS_KILL_REAP_SECONDS,
        )
        return
    _signal_process_group(process_group_id, signal.SIGTERM)
    terminated = await _wait_and_discard_process_output(
        process,
        timeout_seconds=_PROCESS_TERMINATION_GRACE_SECONDS,
    )
    if not terminated or _process_group_exists(process_group_id):
        _signal_process_group(process_group_id, _sigkill)
        await _wait_and_discard_process_output(
            process,
            timeout_seconds=_PROCESS_KILL_REAP_SECONDS,
        )


def _signal_process_group(process_group_id: int, signal_number: int) -> None:
    """@brief 向独立编译进程组发送信号 / Send a signal to the isolated compiler process group.

    @param process_group_id 独立 session 的 PGID / PGID of the isolated session.
    @param signal_number 要发送的 POSIX 信号 / POSIX signal to send.
    @return 无返回值 / No return value.
    @note 进程组已经退出不是失败；这使成功、超时、取消和错误路径能安全共享清理逻辑。
    """

    if _killpg is None:
        raise RuntimeError("POSIX process-group signaling is unavailable on this platform")
    try:
        _killpg(process_group_id, signal_number)
    except ProcessLookupError:
        return
    except PermissionError:
        return


async def _wait_and_discard_process_output(
    process: asyncio.subprocess.Process,
    *,
    timeout_seconds: float,
) -> bool:
    """@brief 等待退出时丢弃剩余 pipe 内容 / Wait for exit while discarding remaining pipe data.

    @param process 已被要求终止的编译子进程 / Compiler subprocess already asked to terminate.
    @param timeout_seconds 此回收阶段的最长时长 / Maximum duration for this reaping phase.
    @return 编译器进程与两个 pipe 都在时限内结束时为真 / True if process and both pipes finish in time.
    @note ``asyncio.subprocess.Process.wait`` 在满 pipe 时可能等待；并行丢弃未消费的 pipe
    数据使超限或取消路径不会被诊断输出反向阻塞。
    """

    tasks = {
        asyncio.create_task(_wait_for_process_exit(process)),
        *(
            asyncio.create_task(_discard_compiler_pipe(pipe))
            for pipe in (process.stdout, process.stderr)
            if pipe is not None
        ),
    }
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout_seconds)
    except TimeoutError:
        return False
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    return True


async def _discard_compiler_pipe(pipe: asyncio.StreamReader) -> None:
    """@brief 丢弃一个已终止编译器的剩余 pipe 数据 / Discard remaining data from a terminating compiler pipe.

    @param pipe 待丢弃的 stdout 或 stderr / stdout or stderr stream to discard.
    @return 无返回值 / No return value.
    """

    while await pipe.read(_PIPE_READ_CHUNK_BYTES):
        pass


def _process_group_exists(process_group_id: int) -> bool:
    """@brief 检查编译进程组是否仍存在 / Check whether a compiler process group still exists.

    @param process_group_id 独立 session 的 PGID / PGID of the isolated session.
    @return 进程组仍可被信号寻址时为真 / True when the process group can still receive a signal.
    """

    if _killpg is None:
        raise RuntimeError("POSIX process-group signaling is unavailable on this platform")
    try:
        _killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _safe_diagnostic(stderr: bytes) -> str:
    """@brief 截断并脱敏编译诊断 / Truncate and sanitize compiler diagnostics.

    @param stderr 原始 stderr / Raw stderr.
    @return 安全诊断摘要 / Safe diagnostic summary.
    """
    return stderr.decode("utf-8", "replace").replace("/work/", "").replace("\x00", "")[:1000]
