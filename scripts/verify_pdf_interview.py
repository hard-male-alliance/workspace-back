#!/usr/bin/env python3
"""Run deployable PDF, Interview-provider, and API media-shape smoke checks."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, cast

from backend.application.ports.interview_v2 import (
    InterviewWorkerOperationId,
    ReportGenerationRequest,
)
from backend.config import BackendSettings
from backend.domain.interview_v2 import (
    InterviewRubric,
    InterviewSessionId,
    JobTarget,
    RubricDimension,
    ScoreScale,
    TranscriptSegment,
    TranscriptSegmentId,
    TranscriptSpeaker,
)
from backend.domain.principals import WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.interview_report import (
    ModelDataRegion,
    StreamingJsonInterviewReportProvider,
)
from backend.infrastructure.providers import OpenAICompatibleModelProvider
from backend.infrastructure.rendering import renderer_for
from workspace_shared.jsonc import load_jsonc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a real sandboxed PDF render, a real configured Interview report "
            "provider call, and the frozen API V2 input media shapes."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.jsonc"),
        help="Private backend JSONC configuration (default: config.jsonc).",
    )
    parser.add_argument(
        "--pdf-output",
        type=Path,
        default=Path("artifacts/realistic-resume.pdf"),
        help="Where to write the rendered PDF.",
    )
    parser.add_argument(
        "--resume-json-output",
        type=Path,
        default=Path("artifacts/realistic-resume-input.json"),
        help="Where to write the complete Resume SIR used for rendering.",
    )
    parser.add_argument(
        "--interview-json-output",
        type=Path,
        default=Path("artifacts/realistic-interview-report.json"),
        help="Where to write the validated Interview report JSON.",
    )
    parser.add_argument(
        "--interview-markdown-output",
        type=Path,
        default=Path("artifacts/realistic-interview-report.md"),
        help="Where to write a human-readable Interview report.",
    )
    parser.add_argument(
        "--interview-transcript-output",
        type=Path,
        default=Path("artifacts/realistic-interview-transcript.json"),
        help="Where to write the multi-turn Interview transcript used for scoring.",
    )
    parser.add_argument(
        "--contract-only",
        action="store_true",
        help="Only inspect the frozen API contract; do not render or call a provider.",
    )
    parser.add_argument(
        "--skip-contract",
        action="store_true",
        help="Skip contract inspection (useful inside an installed production image).",
    )
    parser.add_argument(
        "--skip-pdf",
        action="store_true",
        help="Skip the real XeLaTeX render.",
    )
    parser.add_argument(
        "--skip-provider",
        action="store_true",
        help="Skip the real Interview report-provider call.",
    )
    return parser


def _contract_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "workspace-shared-docs"
        / "contracts"
        / "v2"
        / "schema.jsonc"
    )


def _definitions() -> dict[str, Any]:
    root = load_jsonc(_contract_path())
    definitions = root.get("$defs")
    if not isinstance(definitions, dict):
        raise RuntimeError("API V2 contract does not contain $defs")
    return definitions


def _reference_name(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    reference = value.get("$ref")
    if not isinstance(reference, str):
        return None
    return reference.rsplit("/", 1)[-1]


def verify_contract_shapes() -> dict[str, object]:
    definitions = _definitions()
    create_message = definitions["CreateMessageRequest"]
    message_items = create_message["properties"]["content"]["items"]
    agent_input = _reference_name(message_items)
    if agent_input != "TextContentPart":
        raise RuntimeError("CreateMessageRequest unexpectedly accepts non-text input")

    upload = definitions["CreateUploadSessionRequest"]
    upload_media_type = upload["properties"]["media_type"]
    if upload_media_type.get("type") != "string":
        raise RuntimeError("Upload media_type is not declared")

    interview = definitions["CreateInterviewSessionRequest"]
    media_reference = _reference_name(interview["properties"]["media"])
    if media_reference != "InterviewMediaPreferences":
        raise RuntimeError("Interview media preferences are not bound to the session request")

    result: dict[str, object] = {
        "agent_message_input": "text content blocks only",
        "knowledge_upload_input": "binary upload session with declared media_type",
        "interview_rest_input": "strict JSON session/control payloads",
        "interview_realtime_input": (
            "candidate text/control JSON frames and binary audio/video media chunks"
        ),
    }
    print(json.dumps({"contract": result}, ensure_ascii=False, sort_keys=True))
    return result


async def verify_pdf(
    settings: BackendSettings,
    output: Path,
    resume_json_output: Path,
) -> dict[str, object]:
    renderer_settings = replace(
        settings.renderer,
        adapter="xelatex",
        artifact_directory=output.parent,
    )
    renderer = renderer_for(renderer_settings, environment="production")
    document = {
        "id": "resume_runtime_verification",
        "revision": 1,
        "artifact_id": "artifact_runtime_verification",
        "title": "高级后端工程师简历",
        "profile": {
            "full_name": "林可莉",
            "headline": "高级后端工程师｜Python · FastAPI · PostgreSQL",
            "summary": {
                "text": (
                    "5 年后端研发经验，专注高并发 API、异步任务与检索增强系统。"
                    "擅长用事务、Outbox、幂等和可观测性建设可靠服务。"
                ),
                "marks": [],
            },
            "contacts": [
                {
                    "id": "contact_email_runtime",
                    "kind": "email",
                    "label": "Email",
                    "value": "klee@example.com",
                    "url": "mailto:klee@example.com",
                },
                {
                    "id": "contact_phone_runtime",
                    "kind": "phone",
                    "label": "Phone",
                    "value": "+86 138-0000-0000",
                    "url": None,
                },
                {
                    "id": "contact_location_runtime",
                    "kind": "location",
                    "label": "Location",
                    "value": "上海",
                    "url": None,
                },
            ],
        },
        "sections": [
            {
                "id": "section_skills_runtime",
                "kind": "skills",
                "title": "核心技能",
                "visible": True,
                "content": {
                    "text": (
                        "Python、FastAPI、PostgreSQL、pgvector、Redis、Docker、"
                        "消息队列、RAG、OAuth 2.0、可观测性"
                    ),
                    "marks": [],
                },
                "items": [],
            },
            {
                "id": "section_experience_runtime",
                "kind": "experience",
                "title": "工作经历",
                "visible": True,
                "content": None,
                "items": [
                    {
                        "id": "item_experience_runtime",
                        "kind": "experience",
                        "title": "高级后端工程师",
                        "subtitle": "AI 平台组",
                        "organization": "示例科技有限公司",
                        "location": "上海",
                        "date_range": {"start": "2022-03", "end": "present"},
                        "summary": {
                            "text": "负责求职工作台、知识检索和异步任务平台的后端设计与交付。",
                            "marks": [],
                        },
                        "highlights": [
                            {
                                "text": (
                                    "设计 PostgreSQL 事务 + Outbox 工作流，使业务写入与任务发布"
                                    "保持原子性，任务重复执行率下降 92%。"
                                ),
                                "marks": [],
                            },
                            {
                                "text": (
                                    "基于 pgvector 和词法检索实现混合 RAG，在离线评测集上将"
                                    " Top-5 召回率从 71% 提升至 89%。"
                                ),
                                "marks": [],
                            },
                            {
                                "text": (
                                    "建设请求追踪、结构化日志和告警，线上故障平均定位时间"
                                    "由 45 分钟降低至 12 分钟。"
                                ),
                                "marks": [],
                            },
                        ],
                        "skills": ["Python", "FastAPI", "PostgreSQL", "pgvector"],
                        "tags": [],
                        "visible": True,
                        "url": "https://example.com/careers",
                    }
                ],
            },
            {
                "id": "section_projects_runtime",
                "kind": "projects",
                "title": "项目经历",
                "visible": True,
                "content": None,
                "items": [
                    {
                        "id": "item_project_runtime",
                        "kind": "project",
                        "title": "AI 求职工作台",
                        "subtitle": "后端负责人",
                        "organization": None,
                        "location": None,
                        "date_range": {"start": "2025-01", "end": "2026-07"},
                        "summary": {
                            "text": (
                                "覆盖简历编辑与 PDF 渲染、知识库 RAG、AI 建议和模拟面试。"
                            ),
                            "marks": [],
                        },
                        "highlights": [
                            {
                                "text": (
                                    "实现严格 JSON Schema 模型输出、服务端 citation 和执行时"
                                    "二次授权，避免模型越权修改简历。"
                                ),
                                "marks": [],
                            },
                            {
                                "text": (
                                    "将文件解析和 XeLaTeX 渲染放入 Landlock/libseccomp"
                                    " 受限子进程，生产能力缺失时 fail closed。"
                                ),
                                "marks": [],
                            },
                        ],
                        "skills": ["RAG", "OAuth 2.0", "Docker", "XeLaTeX"],
                        "tags": [],
                        "visible": True,
                        "url": "https://github.com/example/ai-job-workspace",
                    }
                ],
            },
            {
                "id": "section_education_runtime",
                "kind": "education",
                "title": "教育背景",
                "visible": True,
                "content": None,
                "items": [
                    {
                        "id": "item_education_runtime",
                        "kind": "education",
                        "title": "计算机科学与技术 · 本科",
                        "subtitle": None,
                        "organization": "示例大学",
                        "location": "杭州",
                        "date_range": {"start": "2017-09", "end": "2021-06"},
                        "summary": None,
                        "highlights": [],
                        "skills": [],
                        "tags": [],
                        "visible": True,
                        "url": None,
                    }
                ],
            },
        ],
    }
    resolved_input = await asyncio.to_thread(
        _write_text,
        resume_json_output,
        json.dumps(document, ensure_ascii=False, indent=2) + "\n",
    )
    content, source_map = await renderer.render(document)
    if not content.startswith(b"%PDF-"):
        raise RuntimeError("renderer output does not start with a PDF header")
    if source_map["page_count"] < 1:
        raise RuntimeError("renderer reported an empty PDF")
    resolved_output = await asyncio.to_thread(_write_pdf, output, content)
    result: dict[str, object] = {
        "path": str(resolved_output),
        "bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "pages": source_map["page_count"],
        "renderer": "sandboxed-xelatex",
        "input_path": str(resolved_input),
    }
    print(json.dumps({"pdf": result}, ensure_ascii=False, sort_keys=True))
    return result


def _write_pdf(output: Path, content: bytes) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(content)
    return output.resolve()


def _interview_request() -> ReportGenerationRequest:
    session_id = InterviewSessionId("session_runtime_verification")
    rubric = InterviewRubric(
        "rubric_runtime_verification",
        "1",
        "Backend engineering interview",
        (
            RubricDimension(
                "dimension_runtime_evidence",
                "Technical evidence",
                "Uses concrete technical evidence and explains trade-offs.",
                0.4,
                ("Names a concrete implementation", "Explains one trade-off"),
                ScoreScale(0.0, 100.0),
            ),
            RubricDimension(
                "dimension_runtime_design",
                "System design",
                "Builds a coherent, reliable, and scalable design.",
                0.35,
                ("Defines boundaries", "Handles failure and recovery"),
                ScoreScale(0.0, 100.0),
            ),
            RubricDimension(
                "dimension_runtime_communication",
                "Communication",
                "Communicates decisions clearly and concisely.",
                0.25,
                ("Uses a structured answer", "States assumptions"),
                ScoreScale(0.0, 100.0),
            ),
        ),
        ScoreScale(0.0, 100.0),
    )
    transcript = (
        TranscriptSegment(
            TranscriptSegmentId("segment_runtime_question01"),
            WorkspaceId("workspace_runtime_verification"),
            session_id,
            1,
            ResourceRef("realtime_input", "input_runtime_question01"),
            TranscriptSpeaker.INTERVIEWER,
            0,
            4_000,
            "请介绍一个你负责的高可靠异步任务系统，并说明如何处理重复执行。",
        ),
        TranscriptSegment(
            TranscriptSegmentId("segment_runtime_answer01"),
            WorkspaceId("workspace_runtime_verification"),
            session_id,
            2,
            ResourceRef("realtime_input", "input_runtime_answer01"),
            TranscriptSpeaker.CANDIDATE,
            4_100,
            30_000,
            (
                "我负责过简历渲染和知识索引任务平台。请求事务内同时写业务状态、Job 和"
                " Outbox，worker 用 SKIP LOCKED 获取带租约的事件，事务外执行渲染或嵌入，"
                "再用短事务 CAS 提交结果。因为至少一次投递会产生重复执行，消费者以稳定"
                " operation_id 去重，产物写入也校验 SHA-256。代价是状态机和补偿逻辑更复杂，"
                "但进程崩溃后可以安全重放。"
            ),
        ),
        TranscriptSegment(
            TranscriptSegmentId("segment_runtime_question02"),
            WorkspaceId("workspace_runtime_verification"),
            session_id,
            3,
            ResourceRef("realtime_input", "input_runtime_question02"),
            TranscriptSpeaker.INTERVIEWER,
            31_000,
            35_000,
            "如果数据库已提交，但外部模型调用超时，你如何保证恢复且不重复计费？",
        ),
        TranscriptSegment(
            TranscriptSegmentId("segment_runtime_answer02"),
            WorkspaceId("workspace_runtime_verification"),
            session_id,
            4,
            ResourceRef("realtime_input", "input_runtime_answer02"),
            TranscriptSpeaker.CANDIDATE,
            35_100,
            58_000,
            (
                "我会把模型调用放在事务外，并把 event_id 作为供应商支持时的幂等键。超时"
                "先记录为可重试而不是直接认定失败，租约到期后由 worker 重放。若供应商没有"
                "幂等能力，就需要在本地保留调用账本并设置人工核对边界，因为网络超时无法"
                "证明上游没有完成计费。我会设置最大尝试次数，耗尽后将 Job 闭合为明确失败，"
                "而不是永久停留在 running。"
            ),
        ),
        TranscriptSegment(
            TranscriptSegmentId("segment_runtime_question03"),
            WorkspaceId("workspace_runtime_verification"),
            session_id,
            5,
            ResourceRef("realtime_input", "input_runtime_question03"),
            TranscriptSpeaker.INTERVIEWER,
            59_000,
            63_000,
            "请说明你会如何监控这个系统。",
        ),
        TranscriptSegment(
            TranscriptSegmentId("segment_runtime_answer03"),
            WorkspaceId("workspace_runtime_verification"),
            session_id,
            6,
            ResourceRef("realtime_input", "input_runtime_answer03"),
            TranscriptSpeaker.CANDIDATE,
            63_100,
            82_000,
            (
                "我会监控队列深度、最老事件年龄、租约超时、重试次数和各阶段耗时，并用"
                " request_id、job_id、event_id 串联日志与追踪。告警区分容量问题、外部供应商"
                "错误和不可重试的数据错误。发布前会做 worker 崩溃与网络超时故障注入，确认"
                "重放后最终状态和产物摘要一致。"
            ),
        ),
    )
    return ReportGenerationRequest(
        session_id,
        "zh-CN",
        JobTarget(
            "Backend Engineer",
            "Runtime Verification",
            "Shanghai",
            "Build reliable Python and PostgreSQL services.",
            None,
            "senior",
            ("Python", "PostgreSQL", "distributed systems"),
        ),
        rubric,
        transcript,
    )


async def verify_interview_provider(
    settings: BackendSettings,
    json_output: Path,
    markdown_output: Path,
    transcript_output: Path,
) -> dict[str, object]:
    if settings.ai.provider == "mock":
        raise RuntimeError("real Interview provider verification refuses the mock provider")
    if settings.ai.api_key is None or settings.ai.base_url is None:
        raise RuntimeError("AI provider key/base_url is not configured")
    provider = OpenAICompatibleModelProvider(
        provider=settings.ai.provider,
        model=settings.ai.model,
        base_url=settings.ai.base_url,
        api_key=settings.ai.api_key,
        data_region=settings.ai.data_region,
        connect_timeout_ms=settings.network.connect_timeout_ms,
        read_timeout_ms=settings.interview.report_timeout_ms,
        outbound_proxy_url=settings.network.outbound_proxy_url,
    )
    adapter = StreamingJsonInterviewReportProvider(
        provider,
        engine_version=f"runtime-smoke:{settings.ai.model}",
        model_data_region=cast(ModelDataRegion, settings.ai.data_region),
        allow_external_model_processing=True,
        allow_provider_fallback=False,
        timeout_ms=settings.interview.report_timeout_ms,
    )
    request = _interview_request()
    resolved_transcript = await asyncio.to_thread(
        _write_text,
        transcript_output,
        json.dumps(
            [asdict(segment) for segment in request.transcript],
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
    )
    try:
        report = await adapter.generate(
            request,
            operation_id=InterviewWorkerOperationId(
                "interview.report:runtime_verification"
            ),
        )
    finally:
        await provider.aclose()
    report.validate_against(request.rubric, request.transcript, request.session_id)
    report_payload = asdict(report)
    resolved_json = await asyncio.to_thread(
        _write_text,
        json_output,
        json.dumps(report_payload, ensure_ascii=False, indent=2) + "\n",
    )
    resolved_markdown = await asyncio.to_thread(
        _write_text,
        markdown_output,
        _interview_markdown(report_payload),
    )
    result: dict[str, object] = {
        "provider_call": "real",
        "strict_schema_valid": True,
        "evidence_valid": True,
        "rubric_dimensions": len(report.rubric_scores),
        "overall_score": report.overall_score,
        "limitations": list(report.limitations),
        "json_path": str(resolved_json),
        "markdown_path": str(resolved_markdown),
        "transcript_path": str(resolved_transcript),
    }
    print(json.dumps({"interview": result}, ensure_ascii=False, sort_keys=True))
    return result


def _write_text(output: Path, content: str) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")
    return output.resolve()


def _interview_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# 模拟面试评估报告",
        "",
        f"- 总分：{report['overall_score']}",
        f"- 置信度：{report['overall_confidence']}",
        "",
        "## 总结",
        "",
        report["executive_summary"]["plain_text"],
        "",
        "## 分项评分",
        "",
    ]
    for score in report["rubric_scores"]:
        lines.extend(
            (
                f"### {score['dimension_id']}：{score['score']}",
                "",
                score["summary"]["plain_text"],
                "",
                "**证据**",
                "",
            )
        )
        for evidence in score["evidence"]:
            lines.append(
                f"- `{evidence['segment_id']}` "
                f"({evidence['start_ms']}–{evidence['end_ms']} ms)："
                f"{evidence['quote'] or '未引用原文'}"
            )
        lines.extend(("", "**改进建议**", ""))
        lines.extend(f"- {value}" for value in score["improvement_actions"])
        lines.append("")
    lines.extend(("## 优势", ""))
    lines.extend(f"- {value['plain_text']}" for value in report["strengths"])
    lines.extend(("", "## 改进方向", ""))
    lines.extend(f"- {value['plain_text']}" for value in report["improvements"])
    lines.extend(("", "## 行动计划", ""))
    for action in report["action_plan"]:
        lines.extend(
            (
                f"### [{action['priority']}] {action['title']}",
                "",
                f"- 原因：{action['why']}",
                f"- 练习：{action['practice']}",
                f"- 成功标准：{action['success_criterion']}",
                "",
            )
        )
    lines.extend(("## 限制", ""))
    lines.extend(f"- {value}" for value in report["limitations"])
    return "\n".join(lines) + "\n"


async def _run(arguments: argparse.Namespace) -> int:
    if not arguments.skip_contract:
        verify_contract_shapes()
    if arguments.contract_only:
        return 0
    settings = BackendSettings.from_file(arguments.config)
    if not arguments.skip_pdf:
        await verify_pdf(
            settings,
            arguments.pdf_output,
            arguments.resume_json_output,
        )
    if not arguments.skip_provider:
        await verify_interview_provider(
            settings,
            arguments.interview_json_output,
            arguments.interview_markdown_output,
            arguments.interview_transcript_output,
        )
    return 0


def main() -> int:
    arguments = _parser().parse_args()
    return asyncio.run(_run(arguments))


if __name__ == "__main__":
    raise SystemExit(main())
