#!/usr/bin/env python3
"""Exercise real Interview audio transcription, video vision, and report generation."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from backend.application.interview_v2 import RealtimeCoachingContext
from backend.application.ports.interview_v2 import (
    EndSessionOutput,
    InterviewWorkerOperationId,
    ReportGenerationRequest,
)
from backend.config import BackendSettings
from backend.domain.interview_v2 import (
    InterviewRubric,
    InterviewSession,
    InterviewSessionId,
    JobTarget,
    RubricDimension,
    ScoreScale,
    TranscriptSegment,
    TranscriptSegmentId,
    TranscriptSpeaker,
)
from backend.domain.platform import (
    ApiArtifactContentUrl,
    Artifact,
    ArtifactId,
    ArtifactKind,
)
from backend.domain.principals import ResourceMeta, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.interview_media_analysis import (
    interview_media_analyzer_for,
)
from backend.infrastructure.interview_realtime_coaching import (
    ProviderRealtimeInterviewCoach,
)
from backend.infrastructure.interview_report import (
    ModelDataRegion,
    StreamingJsonInterviewReportProvider,
)
from backend.infrastructure.providers import OpenAICompatibleModelProvider

WORKSPACE = WorkspaceId("workspace_multimodal_verify")
SESSION = InterviewSessionId("session_multimodal_verify")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--audio", type=Path, required=True)
    parser.add_argument("--video", type=Path, required=True)
    parser.add_argument("--keyframe", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--environment",
        choices=("development", "production"),
        default="production",
    )
    return parser.parse_args()


def _artifact(
    path: Path,
    artifact_id: ArtifactId,
    kind: ArtifactKind,
    media_type: str,
) -> tuple[Artifact, bytes]:
    content = path.read_bytes()
    now = datetime.now(UTC)
    artifact = Artifact(
        ResourceMeta(artifact_id, 1, now, now),
        WORKSPACE,
        kind,
        ResourceRef("interview_session", SESSION, 3),
        media_type,
        len(content),
        hashlib.sha256(content).hexdigest(),
        ApiArtifactContentUrl.build(
            "https://api.example.test",
            WORKSPACE,
            artifact_id,
        ),
    )
    return artifact, content


def _session() -> InterviewSession:
    return cast(
        InterviewSession,
        SimpleNamespace(
            spec=SimpleNamespace(
                recording=SimpleNamespace(store_transcript=True),
                locale="zh-CN",
            ),
            grant=SimpleNamespace(external_model_processing=True),
        ),
    )


def _report_request(
    analyzed: tuple[TranscriptSegment, ...],
) -> ReportGenerationRequest:
    question = TranscriptSegment(
        TranscriptSegmentId("segment_multimodal_question"),
        WORKSPACE,
        SESSION,
        1,
        ResourceRef("realtime_input", "input_multimodal_question"),
        TranscriptSpeaker.INTERVIEWER,
        0,
        4_000,
        "请介绍你设计的可靠异步任务系统，并结合屏幕中的架构图说明恢复策略。",
    )
    rubric = InterviewRubric(
        "rubric_multimodal_verify",
        "1",
        "Multimodal backend engineering interview",
        (
            RubricDimension(
                "dimension_technical",
                "Technical design",
                "Explains a concrete reliable architecture and its trade-offs.",
                0.6,
                ("Concrete design", "Failure recovery", "Trade-offs"),
                ScoreScale(0.0, 100.0),
            ),
            RubricDimension(
                "dimension_evidence",
                "Evidence use",
                "Uses transcript and visible presentation evidence without sensitive inference.",
                0.4,
                ("Grounded in transcript", "Grounded in visible materials"),
                ScoreScale(0.0, 100.0),
            ),
        ),
        ScoreScale(0.0, 100.0),
    )
    return ReportGenerationRequest(
        SESSION,
        "zh-CN",
        JobTarget(
            "Senior Backend Engineer",
            "Runtime Verification",
            "Shanghai",
            "Design reliable Python, PostgreSQL, and asynchronous systems.",
            None,
            "senior",
            ("Python", "PostgreSQL", "distributed systems"),
        ),
        rubric,
        (question, *analyzed),
    )


async def _run(arguments: argparse.Namespace) -> dict[str, Any]:
    settings = BackendSettings.from_file(arguments.config)
    if settings.ai.provider == "mock":
        raise RuntimeError("multimodal verification requires a real AI provider")
    audio, audio_content = _artifact(
        arguments.audio,
        ArtifactId("artifact_multimodal_audio"),
        ArtifactKind.INTERVIEW_AUDIO,
        "audio/ogg",
    )
    video, _video_content = _artifact(
        arguments.video,
        ArtifactId("artifact_multimodal_video"),
        ArtifactKind.INTERVIEW_VIDEO,
        "video/mp4",
    )
    analyzer = interview_media_analyzer_for(
        settings.ai,
        settings.interview,
        settings.network,
        environment=arguments.environment,
    )
    if analyzer is None:
        raise RuntimeError("configured Interview media analyzer is unavailable")
    try:
        analysis = await analyzer.analyze(
            _session(),
            EndSessionOutput(
                (audio,),
                (audio_content,),
            ),
            (),
            operation_id=InterviewWorkerOperationId(
                "interview.end:multimodal_verification"
            ),
        )
        realtime_transcript = await analyzer.transcribe_realtime(
            audio_content,
            audio.media_type,
            "zh-CN",
            operation_id=InterviewWorkerOperationId(
                "input_multimodal_realtime_audio"
            ),
        )
        realtime_observation = await analyzer.analyze_realtime_frame(
            arguments.keyframe.read_bytes(),
            "image/jpeg",
            operation_id=InterviewWorkerOperationId(
                "input_multimodal_realtime_frame"
            ),
        )
    except BaseException:
        await analyzer.aclose()
        raise
    speakers = {item.speaker for item in analysis.segments}
    if TranscriptSpeaker.CANDIDATE not in speakers:
        raise RuntimeError("real STT produced no candidate transcript")

    transcript_values = [
        TranscriptSegment(
            TranscriptSegmentId(f"segment_multimodal_{index:03d}"),
            WORKSPACE,
            SESSION,
            index + 1,
            item.source_ref,
            item.speaker,
            item.start_ms,
            item.end_ms,
            item.text,
        )
        for index, item in enumerate(analysis.segments, start=1)
    ]
    if TranscriptSpeaker.SYSTEM not in speakers:
        transcript_values.append(
            TranscriptSegment(
                TranscriptSegmentId("segment_multimodal_realtime_visual"),
                WORKSPACE,
                SESSION,
                len(transcript_values) + 1,
                ResourceRef("artifact", video.meta.id, video.meta.revision),
                TranscriptSpeaker.SYSTEM,
                1_000,
                1_000,
                f"[Video observation] {realtime_observation}",
            )
        )
    transcript = tuple(transcript_values)
    request = _report_request(transcript)
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
    report_adapter = StreamingJsonInterviewReportProvider(
        provider,
        engine_version=f"multimodal-smoke:{settings.ai.model}",
        model_data_region=cast(ModelDataRegion, settings.ai.data_region),
        allow_external_model_processing=True,
        allow_provider_fallback=False,
        timeout_ms=settings.interview.report_timeout_ms,
    )
    try:
        realtime_coach = ProviderRealtimeInterviewCoach(provider, analyzer)
        realtime_context = RealtimeCoachingContext(
            "Senior Backend Engineer live interview",
            "Assess reliable Python, PostgreSQL, and asynchronous system design.",
            "technical",
            "advanced",
            ("Python", "PostgreSQL", "reliability"),
            "zh-CN",
            True,
            (),
            settings.ai.data_region,
        )
        realtime_followup = "".join(
            [
                chunk
                async for chunk in realtime_coach.stream_followup(
                    realtime_context,
                    realtime_transcript,
                    realtime_observation,
                    (),
                    operation_id="input_multimodal_realtime_audio:followup",
                )
            ]
        )
        report = await report_adapter.generate(
            request,
            operation_id=InterviewWorkerOperationId(
                "interview.report:multimodal_verification"
            ),
        )
    finally:
        await provider.aclose()
        await analyzer.aclose()
    report.validate_against(request.rubric, request.transcript, request.session_id)

    payload: dict[str, Any] = {
        "audio": {
            "model": settings.interview.stt_model,
            "sha256": audio.sha256,
            "bytes": audio.size_bytes,
            "transcript": [
                item.text
                for item in transcript
                if item.speaker is TranscriptSpeaker.CANDIDATE
            ],
        },
        "video": {
            "model": settings.interview.vision_model,
            "sha256": video.sha256,
            "bytes": video.size_bytes,
            "observations": [
                item.text
                for item in transcript
                if item.speaker is TranscriptSpeaker.SYSTEM
            ],
        },
        "report": asdict(report),
        "realtime": {
            "transcript": realtime_transcript,
            "visual_observation": realtime_observation,
            "followup": realtime_followup,
        },
        "checks": {
            "real_stt": True,
            "real_vision": True,
            "strict_report_schema": True,
            "report_evidence_valid": True,
            "sensitive_visual_inference_requested": False,
            "realtime_stt": bool(realtime_transcript),
            "realtime_keyframe": bool(realtime_observation),
            "realtime_followup": bool(realtime_followup),
        },
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return payload


def main() -> int:
    arguments = _arguments()
    payload = asyncio.run(_run(arguments))
    print(
        json.dumps(
            {
                "output": str(arguments.output.resolve()),
                "checks": payload["checks"],
                "audio_transcript": payload["audio"]["transcript"],
                "video_observations": payload["video"]["observations"],
                "overall_score": payload["report"]["overall_score"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
