"""Low-latency Interview V2 transcription, visual context, and follow-up generation."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Protocol

from backend.application.interview_v2 import RealtimeCoachingContext
from backend.application.ports.interview_v2 import InterviewWorkerOperationId
from backend.domain.ports import ModelProvider
from backend.infrastructure.interview_media_analysis import (
    OpenRouterInterviewMediaAnalyzer,
)

_MAX_FOLLOWUP_CHARS = 2_000


class RealtimeInterviewCoach(Protocol):
    """Provider-neutral live Interview inference port used by the private socket."""

    async def transcribe_audio(
        self,
        content: bytes,
        media_type: str,
        locale: str,
        *,
        operation_id: str,
    ) -> str: ...

    async def observe_frame(
        self,
        content: bytes,
        media_type: str,
        *,
        operation_id: str,
    ) -> str: ...

    def stream_followup(
        self,
        context: RealtimeCoachingContext,
        candidate_text: str,
        visual_observation: str | None,
        live_history: tuple[tuple[str, str], ...],
        *,
        operation_id: str,
    ) -> AsyncIterator[str]: ...


class ProviderRealtimeInterviewCoach:
    """Combine consent-gated media inference with the configured streaming LLM."""

    def __init__(
        self,
        provider: ModelProvider,
        media_analyzer: OpenRouterInterviewMediaAnalyzer | None,
    ) -> None:
        self._provider = provider
        self._media = media_analyzer

    async def transcribe_audio(
        self,
        content: bytes,
        media_type: str,
        locale: str,
        *,
        operation_id: str,
    ) -> str:
        if self._media is None:
            raise RuntimeError("realtime audio transcription is unavailable")
        return await self._media.transcribe_realtime(
            content,
            media_type,
            locale,
            operation_id=InterviewWorkerOperationId(operation_id),
        )

    async def observe_frame(
        self,
        content: bytes,
        media_type: str,
        *,
        operation_id: str,
    ) -> str:
        if self._media is None:
            raise RuntimeError("realtime visual analysis is unavailable")
        return await self._media.analyze_realtime_frame(
            content,
            media_type,
            operation_id=InterviewWorkerOperationId(operation_id),
        )

    async def stream_followup(
        self,
        context: RealtimeCoachingContext,
        candidate_text: str,
        visual_observation: str | None,
        live_history: tuple[tuple[str, str], ...],
        *,
        operation_id: str,
    ) -> AsyncIterator[str]:
        history = [
            {
                "speaker": item.speaker.value,
                "text": item.text,
            }
            for item in context.transcript
            if item.text.strip()
        ]
        prompt = json.dumps(
            {
                "task": (
                    "Act as the interviewer. Understand the candidate's latest answer and ask "
                    "exactly one concise, relevant follow-up question. Do not score, explain, "
                    "praise, or reveal reasoning. Do not infer sensitive or biometric traits "
                    "from visual context. Return only the question."
                ),
                "scenario": {
                    "name": context.scenario_name,
                    "description": context.scenario_description,
                    "type": context.interview_type,
                    "difficulty": context.difficulty,
                    "focus_areas": list(context.focus_areas),
                },
                "recent_transcript": history,
                "live_connection_history": [
                    {"speaker": speaker, "text": text}
                    for speaker, text in live_history[-40:]
                ],
                "latest_candidate_answer": candidate_text,
                "latest_visual_observation": visual_observation,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        request = {
            "capability": "interview_coach",
            "response_locale": context.locale,
            "output_modes": ["text"],
            "operation_id": operation_id,
            "inference": {
                "data_region": context.data_region,
                "allow_external_model_processing": True,
                "allow_provider_fallback": False,
            },
        }
        emitted = 0
        try:
            async for chunk in self._provider.stream_text(prompt, request):
                if not isinstance(chunk, str):
                    raise RuntimeError("realtime interviewer returned a non-text chunk")
                remaining = _MAX_FOLLOWUP_CHARS - emitted
                if remaining <= 0:
                    raise RuntimeError("realtime interviewer output exceeded its bound")
                value = chunk[:remaining]
                emitted += len(value)
                if value:
                    yield value
                if len(chunk) > remaining:
                    raise RuntimeError("realtime interviewer output exceeded its bound")
        except asyncio.CancelledError:
            raise
        except RuntimeError:
            raise
        except Exception as error:
            raise RuntimeError("realtime interviewer provider failed") from error
        if emitted == 0:
            raise RuntimeError("realtime interviewer returned an empty question")


__all__ = ["ProviderRealtimeInterviewCoach", "RealtimeInterviewCoach"]
