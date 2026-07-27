"""Load and render backend-owned Agent prompts."""

from __future__ import annotations

from functools import lru_cache
from importlib.resources import files

_RESUME_AGENT_PROMPT = "resume_agent_system.md"


@lru_cache(maxsize=1)
def resume_agent_prompt_template() -> str:
    """Return the immutable Resume-Agent system prompt resource."""

    prompt = (
        files("backend.resources.prompts")
        .joinpath(_RESUME_AGENT_PROMPT)
        .read_text(encoding="utf-8")
        .strip()
    )
    if "{response_locale}" not in prompt:
        raise RuntimeError("Resume Agent prompt is missing response_locale")
    return prompt


def render_resume_agent_system_prompt(*, response_locale: str) -> str:
    """Render the Resume-Agent prompt without accepting arbitrary substitutions."""

    locale = response_locale.strip()
    if not 2 <= len(locale) <= 35:
        raise ValueError("Resume Agent response locale is invalid")
    return resume_agent_prompt_template().format(response_locale=locale)


__all__ = ["render_resume_agent_system_prompt", "resume_agent_prompt_template"]
