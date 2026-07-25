"""Interview V2 STT and sampled-video analysis adapter tests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import httpx
import pytest

from backend.application.ports.interview_v2 import (
    EndSessionOutput,
    InterviewWorkerOperationId,
)
from backend.config import BackendSettings
from backend.domain.interview_v2 import (
    InterviewSession,
    InterviewSessionId,
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
    OpenRouterInterviewMediaAnalyzer,
    SandboxedVideoFrameExtractor,
)

NOW = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WORKSPACE = WorkspaceId("workspace_analysis001")
SESSION = InterviewSessionId("session_analysis0001")


class _Frames:
    def __init__(self, frames: tuple[bytes, ...]) -> None:
        self.frames = frames
        self.calls: list[tuple[bytes, str]] = []

    async def extract(self, content: bytes, media_type: str) -> tuple[bytes, ...]:
        self.calls.append((content, media_type))
        return self.frames


def _settings() -> BackendSettings:
    settings = BackendSettings.from_file(Path("example.jsonc"))
    return replace(
        settings,
        ai=replace(
            settings.ai,
            provider="openrouter",
            api_key="test-key",
            base_url="https://openrouter.example/v1",
        ),
    )


def _session(
    *,
    store_transcript: bool = True,
    external_processing: bool = True,
) -> InterviewSession:
    return cast(
        InterviewSession,
        SimpleNamespace(
            spec=SimpleNamespace(
                recording=SimpleNamespace(store_transcript=store_transcript),
                locale="zh-CN",
            ),
            grant=SimpleNamespace(external_model_processing=external_processing),
        ),
    )


def _artifact(kind: ArtifactKind, media_type: str, content: bytes) -> Artifact:
    artifact_id = ArtifactId(
        "artifact_audioanalysis01"
        if kind is ArtifactKind.INTERVIEW_AUDIO
        else "artifact_videoanalysis01"
    )
    return Artifact(
        ResourceMeta(artifact_id, 1, NOW, NOW),
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


def _analyzer(
    handler: httpx.MockTransport,
    frames: _Frames,
) -> tuple[OpenRouterInterviewMediaAnalyzer, httpx.AsyncClient]:
    settings = _settings()
    client = httpx.AsyncClient(transport=handler)
    analyzer = OpenRouterInterviewMediaAnalyzer(
        settings.ai,
        settings.interview,
        settings.network,
        cast(SandboxedVideoFrameExtractor, frames),
        client=client,
    )
    return analyzer, client


@pytest.mark.asyncio
async def test_audio_is_transcribed_to_candidate_artifact_evidence() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"text": "我用事务 Outbox 保证任务可靠投递。", "usage": {"seconds": 3.25}},
        )

    analyzer, client = _analyzer(httpx.MockTransport(respond), _Frames(()))
    content = b"synthetic-ogg"
    artifact = _artifact(ArtifactKind.INTERVIEW_AUDIO, "audio/ogg", content)
    try:
        result = await analyzer.analyze(
            _session(),
            EndSessionOutput((artifact,), (content,)),
            (),
            operation_id=InterviewWorkerOperationId("interview.end:job_analysis0001"),
        )
    finally:
        await client.aclose()

    assert len(requests) == 1
    assert requests[0].url.path == "/v1/audio/transcriptions"
    payload = json.loads(requests[0].content)
    assert payload["model"] == "openai/whisper-large-v3"
    assert payload["input_audio"]["format"] == "ogg"
    assert payload["language"] == "zh"
    assert requests[0].headers["idempotency-key"].endswith(":stt")
    assert len(result.segments) == 1
    segment = result.segments[0]
    assert segment.speaker is TranscriptSpeaker.CANDIDATE
    assert segment.source_ref == ResourceRef("artifact", artifact.meta.id, 1)
    assert segment.end_ms == 3_250
    assert "Outbox" in segment.text


@pytest.mark.asyncio
async def test_realtime_audio_and_camera_frame_use_bounded_provider_requests() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/audio/transcriptions"):
            return httpx.Response(200, json={"text": "实时回答文本"})
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "observations": [
                                        {
                                            "frame_index": 0,
                                            "description": "候选人展示系统架构图。",
                                        }
                                    ]
                                },
                                ensure_ascii=False,
                            )
                        }
                    }
                ]
            },
        )

    analyzer, client = _analyzer(httpx.MockTransport(respond), _Frames(()))
    try:
        transcript = await analyzer.transcribe_realtime(
            b"webm-utterance",
            "audio/webm",
            "zh-CN",
            operation_id=InterviewWorkerOperationId("input_realtimeaudio01"),
        )
        observation = await analyzer.analyze_realtime_frame(
            b"\xff\xd8jpeg-frame",
            "image/jpeg",
            operation_id=InterviewWorkerOperationId("input_realtimevideo01"),
        )
    finally:
        await client.aclose()

    assert transcript == "实时回答文本"
    assert observation == "候选人展示系统架构图。"
    assert [request.url.path for request in requests] == [
        "/v1/audio/transcriptions",
        "/v1/chat/completions",
    ]
    assert requests[0].headers["idempotency-key"].endswith(":realtime-stt")
    assert requests[1].headers["idempotency-key"].endswith(":realtime-vision")
    vision_payload = json.loads(requests[1].content)
    prompt = vision_payload["messages"][0]["content"][0]["text"]
    assert "Do not infer emotion" in prompt


@pytest.mark.asyncio
async def test_existing_candidate_transcript_prevents_duplicate_stt() -> None:
    def unexpected(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("STT must not be called when a candidate transcript exists")

    analyzer, client = _analyzer(httpx.MockTransport(unexpected), _Frames(()))
    content = b"synthetic-ogg"
    artifact = _artifact(ArtifactKind.INTERVIEW_AUDIO, "audio/ogg", content)
    existing = (
        TranscriptSegment(
            TranscriptSegmentId("segment_existingaudio1"),
            WORKSPACE,
            SESSION,
            1,
            ResourceRef("realtime_input", "input_existingaudio01"),
            TranscriptSpeaker.CANDIDATE,
            0,
            1_000,
            "已有浏览器转写",
        ),
    )
    try:
        result = await analyzer.analyze(
            _session(),
            EndSessionOutput((artifact,), (content,)),
            existing,
            operation_id=InterviewWorkerOperationId("interview.end:job_analysis0002"),
        )
    finally:
        await client.aclose()
    assert result.segments == ()


@pytest.mark.asyncio
async def test_video_frames_become_non_biometric_system_observations() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "observations": [
                                        {
                                            "frame_index": 1,
                                            "description": "候选人指向白板上的架构图。",
                                        }
                                    ]
                                },
                                ensure_ascii=False,
                            )
                        }
                    }
                ]
            },
        )

    frames = _Frames((b"\xff\xd8frame-one", b"\xff\xd8frame-two"))
    analyzer, client = _analyzer(httpx.MockTransport(respond), frames)
    content = b"synthetic-mp4"
    artifact = _artifact(ArtifactKind.INTERVIEW_VIDEO, "video/mp4", content)
    try:
        result = await analyzer.analyze(
            _session(),
            EndSessionOutput((artifact,), (content,)),
            (),
            operation_id=InterviewWorkerOperationId("interview.end:job_analysis0003"),
        )
    finally:
        await client.aclose()

    assert frames.calls == [(content, "video/mp4")]
    assert len(requests) == 1
    payload = json.loads(requests[0].content)
    assert payload["model"] == "qwen/qwen3-vl-8b-instruct"
    assert [part["type"] for part in payload["messages"][0]["content"]] == [
        "text",
        "image_url",
        "image_url",
    ]
    assert "Do not infer emotion" in payload["messages"][0]["content"][0]["text"]
    assert payload["response_format"]["json_schema"]["strict"] is True
    assert len(result.segments) == 1
    segment = result.segments[0]
    assert segment.speaker is TranscriptSpeaker.SYSTEM
    assert segment.start_ms == 5_000
    assert segment.text == "[Video observation] 候选人指向白板上的架构图。"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("store_transcript", "external_processing"),
    [(False, True), (True, False)],
)
async def test_analysis_respects_transcript_consent_and_external_processing_policy(
    store_transcript: bool,
    external_processing: bool,
) -> None:
    def unexpected(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("provider must not receive media without policy permission")

    analyzer, client = _analyzer(httpx.MockTransport(unexpected), _Frames(()))
    content = b"synthetic-ogg"
    artifact = _artifact(ArtifactKind.INTERVIEW_AUDIO, "audio/ogg", content)
    try:
        result = await analyzer.analyze(
            _session(
                store_transcript=store_transcript,
                external_processing=external_processing,
            ),
            EndSessionOutput((artifact,), (content,)),
            (),
            operation_id=InterviewWorkerOperationId("interview.end:job_analysis0004"),
        )
    finally:
        await client.aclose()
    assert result.segments == ()
