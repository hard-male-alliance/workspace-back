"""Consent-gated STT and sampled-video analysis for Interview V2 media."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import shutil
import signal
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

from backend.application.ports.interview_v2 import (
    AnalyzedMediaSegment,
    EndSessionOutput,
    InterviewMediaAnalysis,
    InterviewWorkerOperationId,
    InterviewWorkerPortFailure,
)
from backend.config import AISettings, InterviewSettings, NetworkSettings
from backend.domain.interview_v2 import (
    InterviewSession,
    TranscriptSegment,
    TranscriptSpeaker,
)
from backend.domain.platform import Artifact
from backend.domain.resources import ResourceRef
from backend.infrastructure.process_confinement import (
    ProcessConfinementPlan,
    ProcessConfinementUnavailable,
    confinement_plan_for,
)

_VIDEO_SCHEMA: dict[str, object] = {
    "name": "interview_video_observations",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "observations": {
                "type": "array",
                "maxItems": 120,
                "items": {
                    "type": "object",
                    "properties": {
                        "frame_index": {"type": "integer", "minimum": 0},
                        "description": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 2_000,
                        },
                    },
                    "required": ["frame_index", "description"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["observations"],
        "additionalProperties": False,
    },
}

_VIDEO_PROMPT = """\
Analyze these ordered sampled frames from a job interview. Return only observable, job-relevant
visual evidence. You may describe whether the candidate is visible, whether a whiteboard or shared
technical material is visible, and concrete presentation actions. Do not infer emotion, health,
ethnicity, gender, age, disability, personality, honesty, or other sensitive/biometric traits.
Do not score the candidate. If any architecture diagram, source code, query plan, whiteboard, or
other technical material is visible, return at least one concise observation describing it.
Combine consecutive equivalent frames into concise observations.
"""


class SandboxedVideoFrameExtractor:
    """Extract bounded JPEG samples in a killable, network-denied ffmpeg child."""

    def __init__(
        self,
        settings: InterviewSettings,
        *,
        environment: str,
        confinement_plan: ProcessConfinementPlan | None = None,
        executable: str | None = None,
    ) -> None:
        self._settings = settings
        self._plan = confinement_plan or confinement_plan_for(environment)
        selected = executable or shutil.which(settings.ffmpeg_command)
        if selected is None or os.name != "posix":
            raise RuntimeError("configured Interview video analysis requires ffmpeg and POSIX")
        self._executable = str(Path(selected).resolve())

    async def extract(self, content: bytes, media_type: str) -> tuple[bytes, ...]:
        """Return sampled JPEG frames, rejecting malformed or over-budget video."""
        extension = {"video/webm": "webm", "video/mp4": "mp4"}.get(media_type)
        if extension is None:
            raise InterviewWorkerPortFailure(
                "interview.video_format_unsupported",
                retryable=False,
            )
        if not content or len(content) > self._settings.media_analysis_max_video_bytes:
            raise InterviewWorkerPortFailure(
                "interview.video_analysis_input_too_large",
                retryable=False,
            )
        with tempfile.TemporaryDirectory(prefix="aiws-interview-video-") as temporary:
            workdir = Path(temporary)
            (workdir / f"input.{extension}").write_bytes(content)
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                "-m",
                "backend.infrastructure.interview_video_sandbox",
                self._plan.mode.value,
                str(workdir),
                self._executable,
                extension,
                str(self._settings.video_frame_interval_seconds),
                str(self._settings.video_max_frames),
                str(self._settings.media_analysis_timeout_ms),
                str(self._settings.video_frame_max_bytes),
                cwd=workdir,
                env={
                    "PATH": "/usr/bin:/bin",
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                    "PYTHONPATH": "",
                },
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
            try:
                await asyncio.wait_for(
                    process.wait(),
                    self._settings.media_analysis_timeout_ms / 1_000,
                )
            except TimeoutError as error:
                await _terminate_process_group(process)
                raise InterviewWorkerPortFailure(
                    "interview.video_analysis_timeout",
                    retryable=True,
                ) from error
            finally:
                await _terminate_process_group(process)
            if process.returncode == 70:
                raise InterviewWorkerPortFailure(
                    "interview.video_sandbox_unavailable",
                    retryable=True,
                )
            if process.returncode != 0:
                raise InterviewWorkerPortFailure(
                    "interview.video_invalid",
                    retryable=False,
                )
            frames = await asyncio.to_thread(
                _read_frames,
                workdir,
                self._settings.video_max_frames,
                self._settings.video_frame_max_bytes,
            )
            if not frames:
                raise InterviewWorkerPortFailure(
                    "interview.video_no_frames",
                    retryable=False,
                )
            return frames


class OpenRouterInterviewMediaAnalyzer:
    """Use OpenRouter STT and a vision model to derive bounded textual evidence."""

    def __init__(
        self,
        ai: AISettings,
        interview: InterviewSettings,
        network: NetworkSettings,
        frame_extractor: SandboxedVideoFrameExtractor | None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if ai.api_key is None or ai.base_url is None:
            raise ValueError("Interview media analysis requires an AI API key and base URL")
        self._api_key = ai.api_key
        self._stt_model = interview.stt_model
        self._vision_model = interview.vision_model
        self._settings = interview
        self._frames = frame_extractor
        base = _safe_base_url(ai.base_url)
        self._transcription_endpoint = f"{base}/audio/transcriptions"
        self._chat_endpoint = f"{base}/chat/completions"
        timeout = interview.media_analysis_timeout_ms / 1_000
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                timeout,
                connect=network.connect_timeout_ms / 1_000,
            ),
            follow_redirects=False,
            proxy=network.outbound_proxy_url,
            trust_env=False,
        )
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def analyze(
        self,
        session: InterviewSession,
        media: EndSessionOutput,
        existing_transcript: tuple[TranscriptSegment, ...],
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> InterviewMediaAnalysis:
        if (
            not session.spec.recording.store_transcript
            or not session.grant.external_model_processing
        ):
            return InterviewMediaAnalysis()
        has_candidate_text = any(
            item.speaker is TranscriptSpeaker.CANDIDATE and item.text.strip()
            for item in existing_transcript
        )
        segments: list[AnalyzedMediaSegment] = []
        for artifact, content in zip(media.artifacts, media.contents, strict=True):
            if artifact.kind.value == "interview_audio":
                if self._stt_model is not None and not has_candidate_text:
                    segments.append(
                        await self._transcribe(
                            artifact,
                            content,
                            session.spec.locale,
                            operation_id,
                        )
                    )
            elif artifact.kind.value == "interview_video" and self._vision_model is not None:
                segments.extend(
                    await self._analyze_video(
                        artifact,
                        content,
                        operation_id,
                    )
                )
        return InterviewMediaAnalysis(tuple(segments))

    async def _transcribe(
        self,
        artifact: Artifact,
        content: bytes,
        locale: str,
        operation_id: InterviewWorkerOperationId,
    ) -> AnalyzedMediaSegment:
        if len(content) > self._settings.media_analysis_max_audio_bytes:
            raise InterviewWorkerPortFailure(
                "interview.audio_analysis_input_too_large",
                retryable=False,
            )
        format_name = {
            "audio/webm": "webm",
            "audio/ogg": "ogg",
            "audio/mp4": "m4a",
        }.get(artifact.media_type)
        if format_name is None:
            raise InterviewWorkerPortFailure(
                "interview.audio_format_unsupported",
                retryable=False,
            )
        response = await self._post(
            self._transcription_endpoint,
            {
                "model": self._stt_model,
                "input_audio": {
                    "data": base64.b64encode(content).decode("ascii"),
                    "format": format_name,
                },
                "language": locale.split("-", 1)[0].lower(),
            },
            operation_id=f"{operation_id}:stt",
        )
        text = response.get("text")
        if not isinstance(text, str) or not text.strip() or len(text) > 20_000:
            raise InterviewWorkerPortFailure(
                "interview.stt_invalid_response",
                retryable=False,
            )
        duration_ms = _usage_duration_ms(response.get("usage"))
        return AnalyzedMediaSegment(
            ResourceRef("artifact", artifact.meta.id, artifact.meta.revision),
            TranscriptSpeaker.CANDIDATE,
            0,
            duration_ms,
            text.strip(),
        )

    async def transcribe_realtime(
        self,
        content: bytes,
        media_type: str,
        locale: str,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> str:
        """Transcribe one finalized, bounded live utterance."""
        if self._stt_model is None:
            raise InterviewWorkerPortFailure(
                "interview.stt_unavailable",
                retryable=False,
            )
        if not content or len(content) > self._settings.media_analysis_max_audio_bytes:
            raise InterviewWorkerPortFailure(
                "interview.audio_analysis_input_too_large",
                retryable=False,
            )
        format_name = {
            "audio/webm": "webm",
            "audio/ogg": "ogg",
            "audio/mp4": "m4a",
        }.get(media_type)
        if format_name is None:
            raise InterviewWorkerPortFailure(
                "interview.audio_format_unsupported",
                retryable=False,
            )
        response = await self._post(
            self._transcription_endpoint,
            {
                "model": self._stt_model,
                "input_audio": {
                    "data": base64.b64encode(content).decode("ascii"),
                    "format": format_name,
                },
                "language": locale.split("-", 1)[0].lower(),
            },
            operation_id=f"{operation_id}:realtime-stt",
        )
        text = response.get("text")
        if not isinstance(text, str) or not text.strip() or len(text) > 20_000:
            raise InterviewWorkerPortFailure(
                "interview.stt_invalid_response",
                retryable=False,
            )
        return text.strip()

    async def analyze_realtime_frame(
        self,
        content: bytes,
        media_type: str,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> str:
        """Describe one sampled camera frame using only observable interview evidence."""
        if self._vision_model is None:
            raise InterviewWorkerPortFailure(
                "interview.vision_unavailable",
                retryable=False,
            )
        if media_type not in {"image/jpeg", "image/png", "image/webp"}:
            raise InterviewWorkerPortFailure(
                "interview.video_frame_format_unsupported",
                retryable=False,
            )
        if not content or len(content) > self._settings.video_frame_max_bytes:
            raise InterviewWorkerPortFailure(
                "interview.video_frame_too_large",
                retryable=False,
            )
        response = await self._post(
            self._chat_endpoint,
            {
                "model": self._vision_model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": _VIDEO_PROMPT},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:{media_type};base64,"
                                    + base64.b64encode(content).decode("ascii")
                                },
                            },
                        ],
                    }
                ],
                "stream": False,
                "temperature": 0,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": _VIDEO_SCHEMA,
                },
            },
            operation_id=f"{operation_id}:realtime-vision",
        )
        observations = _message_json(response).get("observations")
        if not isinstance(observations, list):
            raise InterviewWorkerPortFailure(
                "interview.vision_invalid_response",
                retryable=False,
            )
        descriptions: list[str] = []
        for value in observations:
            if not isinstance(value, dict):
                raise InterviewWorkerPortFailure(
                    "interview.vision_invalid_response",
                    retryable=False,
                )
            description = value.get("description")
            if not isinstance(description, str) or not description.strip():
                raise InterviewWorkerPortFailure(
                    "interview.vision_invalid_response",
                    retryable=False,
                )
            descriptions.append(description.strip())
        if not descriptions:
            raise InterviewWorkerPortFailure(
                "interview.vision_invalid_response",
                retryable=False,
            )
        return " ".join(descriptions)[:2_000]

    async def _analyze_video(
        self,
        artifact: Artifact,
        content: bytes,
        operation_id: InterviewWorkerOperationId,
    ) -> tuple[AnalyzedMediaSegment, ...]:
        if self._frames is None:
            raise InterviewWorkerPortFailure(
                "interview.video_analysis_unavailable",
                retryable=False,
            )
        frames = await self._frames.extract(content, artifact.media_type)
        message_content: list[dict[str, object]] = [{"type": "text", "text": _VIDEO_PROMPT}]
        message_content.extend(
            {
                "type": "image_url",
                "image_url": {
                    "url": "data:image/jpeg;base64," + base64.b64encode(frame).decode("ascii")
                },
            }
            for frame in frames
        )
        response = await self._post(
            self._chat_endpoint,
            {
                "model": self._vision_model,
                "messages": [{"role": "user", "content": message_content}],
                "stream": False,
                "temperature": 0,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": _VIDEO_SCHEMA,
                },
            },
            operation_id=f"{operation_id}:vision",
        )
        payload = _message_json(response)
        observations = payload.get("observations")
        if not isinstance(observations, list):
            raise InterviewWorkerPortFailure(
                "interview.vision_invalid_response",
                retryable=False,
            )
        result: list[AnalyzedMediaSegment] = []
        source = ResourceRef("artifact", artifact.meta.id, artifact.meta.revision)
        for value in observations:
            if not isinstance(value, dict) or set(value) != {
                "frame_index",
                "description",
            }:
                raise InterviewWorkerPortFailure(
                    "interview.vision_invalid_response",
                    retryable=False,
                )
            frame_index = value["frame_index"]
            description = value["description"]
            if (
                isinstance(frame_index, bool)
                or not isinstance(frame_index, int)
                or not 0 <= frame_index < len(frames)
                or not isinstance(description, str)
                or not description.strip()
                or len(description) > 2_000
            ):
                raise InterviewWorkerPortFailure(
                    "interview.vision_invalid_response",
                    retryable=False,
                )
            start_ms = frame_index * self._settings.video_frame_interval_seconds * 1_000
            result.append(
                AnalyzedMediaSegment(
                    source,
                    TranscriptSpeaker.SYSTEM,
                    start_ms,
                    start_ms,
                    f"[Video observation] {description.strip()}",
                )
            )
        return tuple(result)

    async def _post(
        self,
        endpoint: str,
        payload: Mapping[str, object],
        *,
        operation_id: str,
    ) -> dict[str, Any]:
        try:
            response = await self._client.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                    "Idempotency-Key": operation_id,
                },
                json=payload,
            )
        except (httpx.TimeoutException, httpx.NetworkError) as error:
            raise InterviewWorkerPortFailure(
                "interview.media_provider_unavailable",
                retryable=True,
            ) from error
        if response.status_code >= 400:
            raise InterviewWorkerPortFailure(
                "interview.media_provider_rejected",
                retryable=response.status_code in {408, 409, 425, 429}
                or response.status_code >= 500,
            )
        try:
            decoded = response.json()
        except ValueError as error:
            raise InterviewWorkerPortFailure(
                "interview.media_provider_invalid_response",
                retryable=False,
            ) from error
        if not isinstance(decoded, dict):
            raise InterviewWorkerPortFailure(
                "interview.media_provider_invalid_response",
                retryable=False,
            )
        return decoded


def interview_media_analyzer_for(
    ai: AISettings,
    interview: InterviewSettings,
    network: NetworkSettings,
    *,
    environment: str,
) -> OpenRouterInterviewMediaAnalyzer | None:
    """Build the configured analyzer or return None when both capabilities are disabled."""
    if interview.stt_model is None and interview.vision_model is None:
        return None
    if ai.provider == "mock" or ai.api_key is None or ai.base_url is None:
        if environment in {"staging", "production"}:
            raise RuntimeError("configured Interview media analysis requires a real provider")
        return None
    frames: SandboxedVideoFrameExtractor | None = None
    if interview.vision_model is not None:
        try:
            plan = confinement_plan_for(environment)
            frames = SandboxedVideoFrameExtractor(
                interview,
                environment=environment,
                confinement_plan=plan,
            )
        except (ProcessConfinementUnavailable, RuntimeError) as error:
            if environment in {"staging", "production"}:
                raise RuntimeError(
                    "configured Interview video analysis requires secure ffmpeg"
                ) from error
    return OpenRouterInterviewMediaAnalyzer(ai, interview, network, frames)


def _safe_base_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme not in {"https", "http"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("media provider base URL is invalid")
    if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("media provider base URL must use HTTPS outside loopback")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _read_frames(
    workdir: Path,
    maximum_frames: int,
    maximum_frame_bytes: int,
) -> tuple[bytes, ...]:
    paths = sorted(workdir.glob("frame-*.jpg"))
    if len(paths) > maximum_frames:
        raise InterviewWorkerPortFailure(
            "interview.video_frame_invalid",
            retryable=False,
        )
    frames: list[bytes] = []
    for path in paths:
        frame = path.read_bytes()
        if not frame.startswith(b"\xff\xd8") or len(frame) > maximum_frame_bytes:
            raise InterviewWorkerPortFailure(
                "interview.video_frame_invalid",
                retryable=False,
            )
        frames.append(frame)
    return tuple(frames)


def _usage_duration_ms(value: object) -> int:
    if not isinstance(value, Mapping):
        return 0
    seconds = value.get("seconds")
    if isinstance(seconds, bool) or not isinstance(seconds, int | float) or seconds < 0:
        return 0
    return min(int(seconds * 1_000), 86_400_000)


def _message_json(response: Mapping[str, object]) -> dict[str, Any]:
    choices = response.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        raise InterviewWorkerPortFailure(
            "interview.vision_invalid_response",
            retryable=False,
        )
    choice = choices[0]
    message = choice.get("message") if isinstance(choice, Mapping) else None
    content = message.get("content") if isinstance(message, Mapping) else None
    if not isinstance(content, str) or len(content.encode("utf-8")) > 262_144:
        raise InterviewWorkerPortFailure(
            "interview.vision_invalid_response",
            retryable=False,
        )
    try:
        payload = json.loads(content)
    except (json.JSONDecodeError, UnicodeError) as error:
        raise InterviewWorkerPortFailure(
            "interview.vision_invalid_response",
            retryable=False,
        ) from error
    if not isinstance(payload, dict) or set(payload) != {"observations"}:
        raise InterviewWorkerPortFailure(
            "interview.vision_invalid_response",
            retryable=False,
        )
    return payload


async def _terminate_process_group(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    await process.wait()


__all__ = [
    "OpenRouterInterviewMediaAnalyzer",
    "SandboxedVideoFrameExtractor",
    "interview_media_analyzer_for",
]
