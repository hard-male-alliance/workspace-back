"""Non-model Agent infrastructure fallbacks.

The model/tool loop lives in ``agents_sdk_provider`` and is owned by the OpenAI Agents SDK.
"""

from __future__ import annotations

from backend.application.ports.agent_v2 import (
    AgentToolDecisionClaim,
    ToolExecutionReceipt,
)
from backend.domain.agent_v2 import AgentProviderRequest, ToolCallBinding
from backend.domain.resources import ResourceRef


class AgentToolExecutionUnavailable(RuntimeError):
    """No trusted external-tool executor is configured for this deployment."""


class UnavailableAgentToolExecutor:
    """Fail closed for external tools that have not been productized."""

    async def execute(
        self,
        dispatch: AgentToolDecisionClaim,
        invocation_ref: ResourceRef,
    ) -> ToolExecutionReceipt:
        del dispatch, invocation_ref
        raise AgentToolExecutionUnavailable("Agent tool execution is not configured")


class EmptyAgentToolRegistry:
    """Explicitly deny unregistered external tools."""

    def allows(self, request: AgentProviderRequest, binding: ToolCallBinding) -> bool:
        del request, binding
        return False


__all__ = [
    "AgentToolExecutionUnavailable",
    "EmptyAgentToolRegistry",
    "UnavailableAgentToolExecutor",
]
