"""@brief 受限简历渲染基础设施 / Restricted resume-rendering infrastructure."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any

from pypdf import PdfReader

from backend.config import RendererSettings
from backend.domain.common import DomainError, Problem
from backend.infrastructure.process_confinement import (
    ProcessConfinementPlan,
    ProcessConfinementUnavailable,
    confinement_plan_for,
)
from workspace_shared.ids import new_opaque_id

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
        worker_bound = "artifact_id" in document
        artifact_id = _required_artifact_id(document)
        nodes = _mock_source_nodes(document, worker_bound=worker_bound)
        source_map = {
            "schema_version": "1.0",
            "resume_id": document["id"],
            "resume_revision": document["revision"],
            "artifact_id": artifact_id,
            "page_count": 1,
            "nodes": nodes,
        }
        return pdf, source_map


def _mock_source_nodes(
    document: dict[str, Any],
    *,
    worker_bound: bool,
) -> list[dict[str, Any]]:
    """@brief 分别投影 V2 canonical 与旧版 mock source nodes / Project V2-canonical and legacy mock source nodes.

    @param document renderer 输入 SIR / Renderer-input SIR.
    @param worker_bound 是否由 V2 worker 预绑定 Artifact ID / Whether the V2 worker pre-bound the Artifact ID.
    @return 对应调用边界的 source nodes / Source nodes for the calling boundary.
    """
    sections = document["sections"]
    if not isinstance(sections, list):
        raise DomainError(
            Problem("resume.invalid_render_input", 422, "Resume sections must be an array")
        )
    nodes: list[dict[str, Any]] = []
    if worker_bound:
        nodes.append(
            {
                "entity_id": document["id"],
                "field_path": ["title"],
                "page": 1,
                "rects": [
                    {
                        "x": 36.0,
                        "y": 760.0,
                        "width": 520.0,
                        "height": 24.0,
                        "unit": "pt",
                    }
                ],
            }
        )
    for index, section in enumerate(sections, start=1):
        if not isinstance(section, dict) or not isinstance(section.get("section_id"), str):
            raise DomainError(
                Problem("resume.invalid_render_input", 422, "Resume section identity is invalid")
            )
        rect = {
            "x": 36.0,
            "y": 760.0 - index * 36.0,
            "width": 520.0,
            "height": 24.0,
            "unit": "pt",
        }
        nodes.append(
            {
                **(
                    {"entity_id": section["section_id"]}
                    if worker_bound
                    else {"node_kind": "section", "node_id": section["section_id"]}
                ),
                "field_path": [],
                "page": 1,
                "rects": [rect],
            }
        )
    return nodes


class SandboxedXeLaTeXRenderer:
    """@brief 仅在已 probe OS confinement 内运行的 XeLaTeX renderer / XeLaTeX renderer running only inside probed OS confinement.

    @note 生产必需边界是无特权 Landlock/libseccomp；Bubblewrap 仅为真实 probe 后的额外层。
    """

    def __init__(
        self,
        settings: RendererSettings,
        *,
        confinement_plan: ProcessConfinementPlan | None = None,
        xelatex_path: str | None = None,
    ) -> None:
        """@brief 初始化受限 renderer / Initialize the restricted renderer.

        @param settings 编译约束 / Compilation constraints.
        @param confinement_plan 已完成 capability probe 的隔离计划 / Capability-probed confinement plan.
        @param xelatex_path 已解析的 XeLaTeX executable / Resolved XeLaTeX executable.
        """
        self._settings = settings
        self._confinement_plan = confinement_plan or confinement_plan_for("development")
        self._xelatex_path = xelatex_path or shutil.which(settings.xelatex_command)

    async def render(self, document: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
        """@brief 在隔离进程中编译固定模板 / Compile a fixed template in an isolated process.

        @param document 已验证的 SIR / Validated SIR.
        @return PDF bytes and semantic source map / PDF 字节与语义 source map.
        @raise DomainError sandbox 缺失、超时或编译失败时抛出 / Raised for a missing sandbox, timeout, or compilation failure.
        """
        if _killpg is None or _sigkill is None or self._xelatex_path is None:
            raise DomainError(
                Problem(
                    "resume.renderer_sandbox_unavailable",
                    503,
                    "Secure XeLaTeX sandbox is unavailable",
                    detail="Real XeLaTeX rendering requires a POSIX process sandbox.",
                )
            )
        latex = _safe_template(document)
        if len(latex.encode("utf-8")) > self._settings.max_input_bytes:
            raise DomainError(Problem("resume.input_too_large", 413, "Resume render input is too large"))
        with tempfile.TemporaryDirectory(prefix="aiws-xelatex-") as temporary_directory:
            workdir = Path(temporary_directory)
            source_path = workdir / "resume.tex"
            source_path.write_text(latex, encoding="utf-8")
            argv = _renderer_process_argv(
                self._xelatex_path,
                self._settings,
                workdir,
                self._confinement_plan,
            )
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=workdir,
                env=_renderer_process_environment(workdir),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
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
                if process.returncode == 70:
                    raise DomainError(
                        Problem(
                            "resume.renderer_sandbox_unavailable",
                            503,
                            "Secure XeLaTeX sandbox is unavailable",
                            retryable=True,
                        )
                    )
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
        artifact_id = _required_artifact_id(document)
        page_count = _pdf_page_count(content)
        source_map = {
            "schema_version": "1.0",
            "resume_id": document["id"],
            "resume_revision": document["revision"],
            "artifact_id": artifact_id,
            "page_count": page_count,
            "nodes": [],
        }
        return content, source_map


def renderer_for(
    settings: RendererSettings,
    *,
    environment: str,
) -> MockRenderer | SandboxedXeLaTeXRenderer:
    """@brief 按配置选择 renderer / Select a renderer from configuration.

    @param settings 渲染设置 / Renderer settings.
    @param environment 已验证部署环境 / Validated deployment environment.
    @return 私有 renderer 实现 / Private renderer implementation.
    """
    if settings.adapter == "mock":
        return MockRenderer()
    if _killpg is None or _sigkill is None:
        raise RuntimeError("configured XeLaTeX rendering requires POSIX process controls")
    xelatex_path = shutil.which(settings.xelatex_command)
    if xelatex_path is None:
        raise RuntimeError("configured XeLaTeX executable is unavailable")
    try:
        confinement_plan = confinement_plan_for(environment)
    except ProcessConfinementUnavailable as error:
        raise RuntimeError("configured XeLaTeX rendering requires strong confinement") from error
    return SandboxedXeLaTeXRenderer(
        settings,
        confinement_plan=confinement_plan,
        xelatex_path=xelatex_path,
    )


def _required_artifact_id(document: dict[str, Any]) -> str:
    """@brief 优先读取 worker 预分配 ID，否则为旧调用生成 ID / Prefer the worker-preallocated ID, otherwise generate one for legacy callers.

    @param document renderer 输入 envelope / Renderer input envelope.
    @return 非空 Artifact ID；V2 worker 路径始终稳定 / Non-empty Artifact ID, always stable on the V2 worker path.
    @raise DomainError 显式 ID 非字符串或为空 / Raised when an explicit ID is non-string or empty.
    """
    value = document.get("artifact_id")
    if value is None:
        return new_opaque_id("art")
    if not isinstance(value, str) or not value:
        raise DomainError(
            Problem(
                "resume.artifact_identity_missing",
                500,
                "Renderer input is missing its Artifact identity",
            )
        )
    return value


def _pdf_page_count(content: bytes) -> int:
    """@brief 为 renderer source map 读取实际 PDF 页数 / Read the actual PDF page count for the renderer source map.

    @param content 受输出大小限制的 PDF / Output-size-bounded PDF.
    @return 至少为一的页数 / Page count of at least one.
    @raise DomainError PDF 结构无效或没有页面 / Raised when the PDF structure is invalid or empty.
    """
    try:
        page_count = len(PdfReader(BytesIO(content)).pages)
    except Exception as error:
        raise DomainError(
            Problem("resume.invalid_pdf", 422, "Renderer produced an invalid PDF")
        ) from error
    if page_count < 1:
        raise DomainError(
            Problem("resume.invalid_pdf", 422, "Renderer produced a PDF without pages")
        )
    return page_count


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
    profile = _mapping(document.get("profile"))
    document_title = _latex_escape(_text(document.get("title")))
    full_name = _latex_escape(_text(profile.get("full_name")))
    headline = _latex_paragraph(_text(profile.get("headline")))
    profile_summary = _latex_rich_text(profile.get("summary"))
    contacts = tuple(
        rendered
        for value in _sequence(profile.get("contacts"))
        if (rendered := _latex_contact(value))
    )
    body: list[str] = [
        "\\begin{center}\n",
        f"{{\\LARGE\\bfseries {full_name}}}\\\\[4pt]\n",
    ]
    if headline:
        body.append(f"{{\\large {headline}}}\\\\[4pt]\n")
    if document_title:
        body.append(f"{{\\small {document_title}}}\\\\[4pt]\n")
    if contacts:
        body.append(f"{{\\small {' \\quad | \\quad '.join(contacts)}}}\n")
    body.append("\\end{center}\n")
    if profile_summary:
        body.extend(
            (
                "\\section*{Professional Summary}\n",
                f"{profile_summary}\n",
            )
        )
    for raw_section in _sequence(document.get("sections")):
        section = _mapping(raw_section)
        if not section or section.get("visible") is False:
            continue
        section_title = _latex_escape(_text(section.get("title")))
        content = _latex_rich_text(section.get("content"))
        items = tuple(
            rendered
            for value in _sequence(section.get("items"))
            if (rendered := _latex_item(value))
        )
        if not section_title or (not content and not items):
            continue
        body.append(f"\\section*{{{section_title}}}\n")
        if content:
            body.append(f"{content}\n")
        body.extend(items)
    return (
        "\\documentclass[10pt]{article}\n"
        "\\usepackage{fontspec}\n"
        "\\usepackage[margin=16mm]{geometry}\n"
        "\\setmainfont{Noto Sans CJK SC}\n"
        "\\setlength{\\parindent}{0pt}\n"
        "\\setlength{\\parskip}{4pt}\n"
        "\\setcounter{secnumdepth}{0}\n"
        "\\pagestyle{plain}\n"
        "\\begin{document}\n"
        + "".join(body)
        + "\\end{document}\n"
    )


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _sequence(value: object) -> tuple[object, ...]:
    return tuple(value) if isinstance(value, list | tuple) else ()


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _latex_paragraph(value: str) -> str:
    normalized = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    return "\\par\n".join(_latex_escape(part.strip()) for part in normalized.split("\n"))


def _latex_rich_text(value: object) -> str:
    return _latex_paragraph(_text(_mapping(value).get("text")))


def _latex_contact(value: object) -> str:
    contact = _mapping(value)
    raw_value = _text(contact.get("value")).strip()
    if not raw_value:
        return ""
    label = _text(contact.get("label")).strip() or _text(contact.get("kind")).strip()
    rendered_value = _latex_escape(raw_value)
    return (
        f"{_latex_escape(label)}: {rendered_value}"
        if label
        else rendered_value
    )


def _latex_item(value: object) -> str:
    item = _mapping(value)
    if not item or item.get("visible") is False:
        return ""
    title = _latex_escape(_text(item.get("title")))
    organization = _latex_escape(_text(item.get("organization")))
    subtitle = _latex_escape(_text(item.get("subtitle")))
    location = _latex_escape(_text(item.get("location")))
    date_range = _latex_date_range(item.get("date_range"))
    primary = " — ".join(part for part in (title, organization) if part)
    secondary = " · ".join(part for part in (subtitle, location) if part)
    lines: list[str] = []
    if primary or date_range:
        lines.append(
            f"\\textbf{{{primary}}}"
            + (f"\\hfill {date_range}" if date_range else "")
            + "\\\\\n"
        )
    if secondary:
        lines.append(f"\\textit{{{secondary}}}\\\\\n")
    summary = _latex_rich_text(item.get("summary"))
    if summary:
        lines.append(f"{summary}\n")
    highlights = tuple(
        rendered
        for raw_highlight in _sequence(item.get("highlights"))
        if (rendered := _latex_rich_text(raw_highlight))
    )
    if highlights:
        lines.append("\\begin{itemize}\n")
        lines.extend(f"\\item {highlight}\n" for highlight in highlights)
        lines.append("\\end{itemize}\n")
    skills = tuple(
        _latex_escape(raw_skill)
        for raw_value in _sequence(item.get("skills"))
        if (raw_skill := _text(raw_value).strip())
    )
    if skills:
        lines.append(f"\\textbf{{Skills:}} {', '.join(skills)}\n")
    url = _text(item.get("url")).strip()
    if url:
        lines.append(f"\\textbf{{Link:}} {_latex_escape(url)}\n")
    lines.append("\\medskip\n")
    return "".join(lines)


def _latex_date_range(value: object) -> str:
    date_range = _mapping(value)
    if not date_range:
        return ""
    start = _text(date_range.get("start")).strip()
    end = _text(date_range.get("end")).strip()
    if not start and not end:
        return ""
    rendered_start = _latex_escape(start)
    rendered_end = _latex_escape("Present" if end == "present" else end)
    return " -- ".join(part for part in (rendered_start, rendered_end) if part)


def _latex_escape(value: str) -> str:
    """@brief 转义用户文本而非解释为 TeX / Escape user text instead of interpreting it as TeX.

    @param value 未信任文本 / Untrusted text.
    @return 转义结果 / Escaped result.
    """
    replacements = {"\\": "\\textbackslash{}", "{": "\\{", "}": "\\}", "#": "\\#", "$": "\\$", "%": "\\%", "&": "\\&", "_": "\\_", "^": "\\textasciicircum{}", "~": "\\textasciitilde{}"}
    return "".join(replacements.get(character, character) for character in value)


def _renderer_process_argv(
    xelatex_path: str,
    settings: RendererSettings,
    workdir: Path,
    confinement_plan: ProcessConfinementPlan,
) -> list[str]:
    """@brief 构造 direct strong 或经 probe Bubblewrap 的 launcher / Build a direct-strong or probed-Bubblewrap launcher.

    @param xelatex_path 已解析 XeLaTeX executable / Resolved XeLaTeX executable.
    @param settings 渲染限制 / Render limits.
    @param workdir 私有工作目录 / Private work directory.
    @param confinement_plan 已 probe 的隔离计划 / Probed confinement plan.
    @return 可直接 exec 的 argv / Directly executable argv.
    """

    direct = _renderer_child_argv(xelatex_path, settings, workdir, confinement_plan)
    if confinement_plan.bubblewrap is None:
        return direct
    confined = _renderer_child_argv(
        xelatex_path,
        settings,
        Path("/work"),
        confinement_plan,
    )
    return _bubblewrap_argv(
        confinement_plan.bubblewrap,
        settings,
        workdir,
        child_argv=confined,
        xelatex_path=xelatex_path,
    )


def _renderer_child_argv(
    xelatex_path: str,
    settings: RendererSettings,
    workdir: Path,
    confinement_plan: ProcessConfinementPlan,
) -> list[str]:
    """@brief 构造先限制再 exec XeLaTeX 的 fresh Python child / Build a fresh Python child that confines before execing XeLaTeX.

    @param xelatex_path 已解析 XeLaTeX executable / Resolved XeLaTeX executable.
    @param settings 渲染限制 / Render limits.
    @param workdir child 可见的工作目录 / Child-visible work directory.
    @param confinement_plan 必须实施的隔离计划 / Confinement plan that must be enforced.
    @return renderer sandbox argv / Renderer-sandbox argv.
    """

    font_directories = [
        str(Path(directory).resolve())
        for directory in settings.allowed_font_directories
        if Path(directory).is_dir()
    ]
    return [
        sys.executable,
        "-I",
        "-B",
        "-m",
        "backend.infrastructure.resume_render_sandbox",
        confinement_plan.mode.value,
        str(workdir),
        xelatex_path,
        str(settings.memory_limit_bytes),
        str(settings.max_output_bytes),
        str(settings.timeout_ms),
        json.dumps(font_directories, separators=(",", ":")),
    ]


def _bubblewrap_argv(
    bubblewrap: str,
    settings: RendererSettings,
    workdir: Path,
    *,
    child_argv: Sequence[str],
    xelatex_path: str,
) -> list[str]:
    """@brief 生成真实 probe 后才使用的额外 Bubblewrap 层 / Build the extra Bubblewrap layer used only after a real probe.

    @param bubblewrap bwrap 可执行路径 / bwrap executable path.
    @param settings 渲染限制 / Render limits.
    @param workdir 临时工作目录 / Temporary work directory.
    @param child_argv 内层 Landlock/libseccomp launcher / Inner Landlock/libseccomp launcher.
    @param xelatex_path XeLaTeX executable path / XeLaTeX executable 路径.
    @return 不含 shell 的 argv / Shell-free argv.
    """
    argv = [
        bubblewrap,
        "--die-with-parent",
        "--new-session",
        "--unshare-user",
        "--unshare-ipc",
        "--unshare-net",
        "--unshare-pid",
        "--unshare-uts",
        "--clearenv",
        "--setenv",
        "HOME",
        "/work",
        "--setenv",
        "LANG",
        "C.UTF-8",
        "--setenv",
        "LC_ALL",
        "C.UTF-8",
        "--setenv",
        "PATH",
        f"{sys.prefix}/bin:{os.defpath}",
        "--setenv",
        "PYTHONHASHSEED",
        "0",
        "--setenv",
        "TMPDIR",
        "/work/tmp",
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
        "--dir",
        "/etc",
        "--ro-bind-try",
        "/etc/fonts",
        "/etc/fonts",
        "--ro-bind-try",
        "/etc/texmf",
        "/etc/texmf",
        "--dir",
        "/var",
        "--dir",
        "/var/cache",
        "--ro-bind-try",
        "/var/cache/fontconfig",
        "/var/cache/fontconfig",
        "--dir",
        "/var/lib",
        "--ro-bind-try",
        "/var/lib/texmf",
        "/var/lib/texmf",
        "--bind",
        str(workdir),
        "/work",
        "--chdir",
        "/work",
    ]
    for runtime_root in _renderer_runtime_roots(xelatex_path):
        argv.extend(_bubblewrap_destination_directories(runtime_root))
        argv.extend(["--ro-bind", str(runtime_root), str(runtime_root)])
    for font_directory in settings.allowed_font_directories:
        source = Path(font_directory)
        if source.is_dir():
            argv.extend(["--ro-bind", str(source.resolve()), str(source.resolve())])
    argv.extend(child_argv)
    return argv


def _renderer_runtime_roots(xelatex_path: str) -> tuple[Path, ...]:
    """@brief 返回 Bubblewrap 中 `/usr` 外的 runtime roots / Return runtime roots outside `/usr` for Bubblewrap.

    @param xelatex_path XeLaTeX executable / XeLaTeX executable.
    @return 稳定去重的额外只读 roots / Stable deduplicated extra read-only roots.
    """

    roots: set[Path] = set()
    for candidate in (
        Path(sys.prefix).resolve(),
        Path(sys.base_prefix).resolve(),
        Path(xelatex_path).resolve().parent,
    ):
        try:
            candidate.relative_to("/usr")
        except ValueError:
            roots.add(candidate)
    return tuple(sorted(roots, key=str))


def _bubblewrap_destination_directories(path: Path) -> list[str]:
    """@brief 在空 mount namespace 中创建 mount parent / Create mount parents in an empty namespace.

    @param path 待挂载绝对路径 / Absolute path to mount.
    @return 从根到叶的 ``--dir`` argv / Root-to-leaf ``--dir`` argv.
    """

    arguments: list[str] = []
    for parent in reversed(path.parents):
        if parent == Path("/"):
            continue
        arguments.extend(["--dir", str(parent)])
    return arguments


def _renderer_process_environment(workdir: Path) -> dict[str, str]:
    """@brief 为 launcher 构造不含 backend secrets 的 environment / Build a launcher environment without backend secrets.

    @param workdir 私有工作目录 / Private work directory.
    @return 最小 Python launcher environment / Minimal Python-launcher environment.
    """

    private_directory = str(workdir)
    return {
        "HOME": private_directory,
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": os.defpath,
        "PYTHONHASHSEED": "0",
        "TMPDIR": private_directory,
    }


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
