"""Provider-neutral realtime Interview follow-up tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from backend.application.interview_v2 import RealtimeCoachingContext
from backend.infrastructure.interview_realtime_coaching import (
    ProviderRealtimeInterviewCoach,
)


@dataclass(slots=True)
class _Provider:
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    async def stream_text(
        self,
        prompt: str,
        request: dict[str, Any],
    ) -> AsyncIterator[str]:
        self.calls.append((prompt, request))
        yield "你如何"
        yield "验证该结论？"


@pytest.mark.asyncio
async def test_followup_stream_uses_frozen_policy_and_visual_context() -> None:
    provider = _Provider()
    coach = ProviderRealtimeInterviewCoach(provider, None)
    context = RealtimeCoachingContext(
        "Backend",
        "Backend practical interview",
        "technical",
        "advanced",
        ("Python", "PostgreSQL"),
        "zh-CN",
        True,
        (),
        "global",
    )

    chunks = [
        value
        async for value in coach.stream_followup(
            context,
            "我通过慢查询日志发现缺少索引。",
            "候选人展示查询计划。",
            (("interviewer", "你如何定位性能瓶颈？"),),
            operation_id="input_followup0001:followup",
        )
    ]

    assert chunks == ["你如何", "验证该结论？"]
    prompt, request = provider.calls[0]
    assert "慢查询日志" in prompt
    assert "候选人展示查询计划" in prompt
    assert "你如何定位性能瓶颈" in prompt
    assert request["capability"] == "interview_coach"
    assert request["inference"] == {
        "data_region": "global",
        "allow_external_model_processing": True,
        "allow_provider_fallback": False,
    }
