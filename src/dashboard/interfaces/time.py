"""@brief Dashboard 入口共享的时间解析 / Time parsing shared by Dashboard interfaces."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from dashboard.domain.model import TimeWindow

_DURATION_PATTERN = re.compile(r"^(?P<amount>[1-9][0-9]*)(?P<unit>[smhdw])$")
"""@brief 简洁 --since 时长格式 / Compact ``--since`` duration syntax."""

_DURATION_SECONDS = {
    "s": 1,
    "m": 60,
    "h": 3_600,
    "d": 86_400,
    "w": 604_800,
}
"""@brief 时长单位到秒的固定映射 / Fixed duration-unit-to-seconds mapping."""


def parse_duration(value: str) -> timedelta:
    """@brief 解析人类友好的单段时长 / Parse a human-friendly single-part duration.

    @param value 例如 30m、6h、7d / For example ``30m``, ``6h``, or ``7d``.
    @return 正的 timedelta / Positive ``timedelta``.
    """

    if not isinstance(value, str):
        raise ValueError("--since 必须是时长文本，例如 30m、6h 或 7d。")
    match = _DURATION_PATTERN.fullmatch(value.strip().lower())
    if match is None:
        raise ValueError("--since 必须是正整数加 s/m/h/d/w，例如 30m。")
    seconds = int(match.group("amount")) * _DURATION_SECONDS[match.group("unit")]
    return timedelta(seconds=seconds)


def parse_datetime(value: str, option_name: str) -> datetime:
    """@brief 解析带时区 RFC 3339 时间 / Parse a timezone-aware RFC 3339 timestamp.

    @param value RFC 3339 文本 / RFC 3339 text.
    @param option_name 用于错误信息的参数名 / Option name used in errors.
    @return UTC datetime / UTC ``datetime``.
    """

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as error:
        raise ValueError(f"{option_name} 必须是 RFC 3339 时间戳。") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{option_name} 必须包含时区，例如 2026-01-01T00:00:00Z。")
    return parsed.astimezone(UTC)


def resolve_window(
    *,
    since: str | None,
    start_at: str | None,
    end_at: str | None,
    now: datetime | None = None,
) -> TimeWindow | None:
    """@brief 将启发式与精确时间参数收敛为窗口 / Resolve heuristic and exact time arguments into a window.

    @param since 相对窗口简写 / Relative-window shorthand.
    @param start_at 可选精确起点 / Optional exact start.
    @param end_at 可选精确终点 / Optional exact end.
    @param now 可注入当前时刻 / Optional injected current time.
    @return 未指定时为 None，否则为半开窗口 / ``None`` when omitted, otherwise a half-open window.
    """

    if since is not None and start_at is not None:
        raise ValueError("--since 与 --start-at 不能同时使用。")
    resolved_end = parse_datetime(end_at, "--end-at") if end_at is not None else None
    if since is not None:
        anchor = resolved_end or _aware_now(now)
        return TimeWindow.ending_at(anchor, parse_duration(since))
    if start_at is not None:
        return TimeWindow(parse_datetime(start_at, "--start-at"), resolved_end or _aware_now(now))
    if resolved_end is not None:
        raise ValueError("单独使用 --end-at 没有明确窗口；请同时提供 --since 或 --start-at。")
    return None


def _aware_now(value: datetime | None) -> datetime:
    """@brief 返回已校验 UTC 当前时刻 / Return a validated current UTC time.

    @param value 可选注入时刻 / Optional injected time.
    @return UTC datetime / UTC ``datetime``.
    """

    resolved = datetime.now(UTC) if value is None else value
    if resolved.tzinfo is None:
        raise ValueError("当前时钟必须携带时区。")
    return resolved.astimezone(UTC)


__all__ = ["parse_datetime", "parse_duration", "resolve_window"]
