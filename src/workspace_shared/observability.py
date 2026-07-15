"""@brief 可观测性稳定数据契约 / Stable observability data contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

TelemetryKind = Literal["metric", "log", "span"]


@dataclass(frozen=True, slots=True)
class TelemetryRecord:
    """@brief 可持久化业务 telemetry 记录 / Persistable business telemetry record.

    @note attributes 必须是低基数属性；调用方不得放入 prompt、URL 或自由文本。
    """

    occurred_at: datetime
    kind: TelemetryKind
    actor_id: str
    workspace_id: str
    resource_owner_id: str
    service: str
    name: str
    value: float | None
    request_id: str | None
    attributes: dict[str, str | int | float | bool]
