"""Dashboard 独立使用的轻量领域模型（domain models）。

本模块刻意不复用 backend 或 dbctl 的业务对象。Dashboard 只消费稳定的
可观测性读模型（read model），因此这些模型既可由内存仓库实现，也可由数据库
视图适配器实现。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from math import isfinite
from types import MappingProxyType

from .errors import DashboardValidationError


class MetricKind(StrEnum):
    """@brief Google SRE 四类信号可使用的指标种类（metric kind）。

    `requests` 与 `errors` 是查询窗口内的增量计数；`latency_ms` 是单次或
    已聚合请求的延迟观测值；`saturation` 是介于 0 与 1 的资源饱和度。
    """

    REQUESTS = "requests"
    ERRORS = "errors"
    LATENCY_MS = "latency_ms"
    SATURATION = "saturation"


class HealthStatus(StrEnum):
    """@brief 服务健康状态（health status）的稳定枚举。"""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    NO_DATA = "no_data"


@dataclass(frozen=True, slots=True)
class DashboardScope:
    """@brief Dashboard 读取必须携带的工作区范围（workspace scope）。

    @param workspace_id: 必填的租户工作区标识，禁止以空值表示“所有工作区”。
    @param actor_id: 可选的操作者标识，用于审计边界而非数据过滤。
    """

    workspace_id: str
    actor_id: str | None = None

    def __post_init__(self) -> None:
        """@brief 规范化并校验多租户范围（multi-tenant scope）。

        @return: 无返回值；不满足约束时抛出 DashboardValidationError。
        """

        if not isinstance(self.workspace_id, str):
            raise DashboardValidationError("workspace_id 必须是字符串。")
        if self.actor_id is not None and not isinstance(self.actor_id, str):
            raise DashboardValidationError("actor_id 必须是字符串或 None。")
        workspace_id = self.workspace_id.strip()
        if not workspace_id:
            raise DashboardValidationError("workspace_id 不能为空。")
        object.__setattr__(self, "workspace_id", workspace_id)

        if self.actor_id is not None:
            actor_id = self.actor_id.strip()
            object.__setattr__(self, "actor_id", actor_id or None)

    def to_dict(self) -> dict[str, str | None]:
        """@brief 转换为边界适配器可序列化的字典（dictionary）。

        @return: 含 workspace_id 与 actor_id 的不可歧义范围字典。
        """

        return {"workspace_id": self.workspace_id, "actor_id": self.actor_id}


@dataclass(frozen=True, slots=True)
class MetricSample:
    """@brief 一条已经脱敏的可观测性指标样本（metric sample）。

    @param workspace_id: 样本所属工作区；查询时必须与 DashboardScope 一致。
    @param observed_at: 带时区的观测时刻；构造后统一为 UTC。
    @param service: 产生指标的稳定服务名，不能包含用户自由文本。
    @param metric: 指标种类（MetricKind）。
    @param value: 指标数值；饱和度必须位于 [0, 1]。
    @param dimensions: 低基数维度（dimensions）字典；仅用于受控分组。
    """

    workspace_id: str
    observed_at: datetime
    service: str
    metric: MetricKind
    value: float
    dimensions: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """@brief 执行不可变样本的边界校验与 UTC 归一化。

        @return: 无返回值；无效数据抛出 DashboardValidationError。
        """

        if not isinstance(self.workspace_id, str):
            raise DashboardValidationError("指标样本的 workspace_id 必须是字符串。")
        if not isinstance(self.service, str):
            raise DashboardValidationError("指标样本的 service 必须是字符串。")
        if not isinstance(self.observed_at, datetime):
            raise DashboardValidationError("指标样本的 observed_at 必须是 datetime。")
        if not isinstance(self.dimensions, Mapping):
            raise DashboardValidationError("指标样本的 dimensions 必须是 Mapping。")
        workspace_id = self.workspace_id.strip()
        service = self.service.strip()
        if not workspace_id:
            raise DashboardValidationError("指标样本的 workspace_id 不能为空。")
        if not service:
            raise DashboardValidationError("指标样本的 service 不能为空。")
        if self.observed_at.tzinfo is None:
            raise DashboardValidationError("指标样本的 observed_at 必须携带时区。")

        try:
            metric = MetricKind(self.metric)
        except ValueError as error:
            raise DashboardValidationError(f"不支持的指标种类：{self.metric!r}。") from error

        try:
            value = float(self.value)
        except (TypeError, ValueError) as error:
            raise DashboardValidationError("指标样本的 value 必须是数值。") from error
        if not isfinite(value):
            raise DashboardValidationError("指标样本的 value 必须是有限数值。")
        if value < 0:
            raise DashboardValidationError("指标样本的 value 不能为负数。")
        if metric is MetricKind.SATURATION and value > 1:
            raise DashboardValidationError("saturation 指标必须位于 0 到 1 之间。")

        normalized_dimensions: dict[str, str] = {}
        for key, dimension_value in self.dimensions.items():
            normalized_key = str(key).strip()
            if not normalized_key:
                raise DashboardValidationError("指标维度名称不能为空。")
            normalized_dimensions[normalized_key] = str(dimension_value)

        object.__setattr__(self, "workspace_id", workspace_id)
        object.__setattr__(self, "service", service)
        object.__setattr__(self, "metric", metric)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(UTC))
        object.__setattr__(self, "dimensions", MappingProxyType(normalized_dimensions))

    def to_dict(self) -> dict[str, object]:
        """@brief 转换为 JSON 边界可用的字典（JSON-ready dictionary）。

        @return: 使用 RFC 3339 UTC 时间戳的样本字典。
        """

        return {
            "workspace_id": self.workspace_id,
            "observed_at": self.observed_at.isoformat().replace("+00:00", "Z"),
            "service": self.service,
            "metric": self.metric.value,
            "value": self.value,
            "dimensions": dict(self.dimensions),
        }


@dataclass(frozen=True, slots=True)
class MetricQuery:
    """@brief 仓库读取的有界指标查询（bounded metric query）。

    查询区间采用半开区间 `[start_at, end_at)`，从而相邻窗口可以无重叠地拼接。

    @param scope: 不能为空的 DashboardScope，多租户过滤由仓库强制执行。
    @param start_at: 查询起点，必须带时区。
    @param end_at: 查询终点，必须晚于 start_at，且必须带时区。
    @param service: 可选的单服务过滤条件。
    @param max_samples: 仓库最多返回的样本数量，用来形成背压（backpressure）。
    """

    scope: DashboardScope
    start_at: datetime
    end_at: datetime
    service: str | None = None
    max_samples: int = 10_000

    def __post_init__(self) -> None:
        """@brief 校验窗口边界并规范化时间与服务名。

        @return: 无返回值；不满足查询不变量时抛出 DashboardValidationError。
        """

        if not isinstance(self.scope, DashboardScope):
            raise DashboardValidationError("指标查询必须携带 DashboardScope。")
        if not isinstance(self.start_at, datetime) or not isinstance(self.end_at, datetime):
            raise DashboardValidationError("指标查询的时间边界必须是 datetime。")
        if self.start_at.tzinfo is None or self.end_at.tzinfo is None:
            raise DashboardValidationError("指标查询的时间边界必须携带时区。")
        start_at = self.start_at.astimezone(UTC)
        end_at = self.end_at.astimezone(UTC)
        if start_at >= end_at:
            raise DashboardValidationError("指标查询的 start_at 必须早于 end_at。")
        if (
            isinstance(self.max_samples, bool)
            or not isinstance(self.max_samples, int)
            or self.max_samples < 1
        ):
            raise DashboardValidationError("max_samples 必须是正整数。")

        service = self.service.strip() if self.service is not None else None
        object.__setattr__(self, "start_at", start_at)
        object.__setattr__(self, "end_at", end_at)
        object.__setattr__(self, "service", service or None)


@dataclass(frozen=True, slots=True)
class HealthPolicy:
    """@brief 将 SRE 信号映射为健康状态的阈值策略（health policy）。

    @param warning_error_rate: 进入 degraded 的错误率下界，范围为 [0, 1]。
    @param critical_error_rate: 进入 critical 的错误率下界，范围为 [0, 1]。
    @param warning_latency_ms: 进入 degraded 的 p95 延迟阈值，单位毫秒。
    @param critical_latency_ms: 进入 critical 的 p95 延迟阈值，单位毫秒。
    @param warning_saturation: 进入 degraded 的平均饱和度阈值，范围为 [0, 1]。
    @param critical_saturation: 进入 critical 的平均饱和度阈值，范围为 [0, 1]。
    """

    warning_error_rate: float = 0.01
    critical_error_rate: float = 0.05
    warning_latency_ms: float = 1_000.0
    critical_latency_ms: float = 3_000.0
    warning_saturation: float = 0.70
    critical_saturation: float = 0.90

    def __post_init__(self) -> None:
        """@brief 校验每个阈值范围及警告/严重阈值的单调性。

        @return: 无返回值；非法策略抛出 DashboardValidationError。
        """

        probability_pairs = (
            ("warning_error_rate", self.warning_error_rate),
            ("critical_error_rate", self.critical_error_rate),
            ("warning_saturation", self.warning_saturation),
            ("critical_saturation", self.critical_saturation),
        )
        for name, value in probability_pairs:
            try:
                numeric_value = float(value)
            except (TypeError, ValueError) as error:
                raise DashboardValidationError(f"{name} 必须是有限数值。") from error
            if not isfinite(numeric_value) or not 0 <= numeric_value <= 1:
                raise DashboardValidationError(f"{name} 必须位于 0 到 1 之间。")
            object.__setattr__(self, name, numeric_value)

        for name in ("warning_latency_ms", "critical_latency_ms"):
            try:
                numeric_value = float(getattr(self, name))
            except (TypeError, ValueError) as error:
                raise DashboardValidationError(f"{name} 必须是有限数值。") from error
            if not isfinite(numeric_value) or numeric_value <= 0:
                raise DashboardValidationError(f"{name} 必须为正的有限数值。")
            object.__setattr__(self, name, numeric_value)

        if self.warning_error_rate > self.critical_error_rate:
            raise DashboardValidationError("warning_error_rate 不能大于 critical_error_rate。")
        if self.warning_latency_ms > self.critical_latency_ms:
            raise DashboardValidationError("warning_latency_ms 不能大于 critical_latency_ms。")
        if self.warning_saturation > self.critical_saturation:
            raise DashboardValidationError("warning_saturation 不能大于 critical_saturation。")


@dataclass(frozen=True, slots=True)
class ServiceSummary:
    """@brief 一个服务在查询窗口内的聚合 SRE 摘要（service summary）。

    @param service: 稳定服务名。
    @param request_count: 窗口内请求增量总和。
    @param error_count: 窗口内错误增量总和。
    @param error_rate: 错误率；没有请求样本时为 None。
    @param availability: 可用性；没有请求样本时为 None。
    @param latency_p50_ms: 延迟中位数；没有延迟样本时为 None。
    @param latency_p95_ms: 延迟 p95；没有延迟样本时为 None。
    @param latency_p99_ms: 延迟 p99；没有延迟样本时为 None。
    @param saturation: 平均饱和度；没有饱和度样本时为 None。
    @param health: 由 HealthPolicy 给出的健康状态。
    @param sample_count: 参与聚合的原始样本数。
    """

    service: str
    request_count: float
    error_count: float
    error_rate: float | None
    availability: float | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    saturation: float | None
    health: HealthStatus
    sample_count: int

    def to_dict(self) -> dict[str, object]:
        """@brief 转换为 CLI、API 与 GUI 共用的序列化摘要。

        @return: 仅包含聚合数据、不包含原始用户内容的字典。
        """

        return {
            "service": self.service,
            "request_count": self.request_count,
            "error_count": self.error_count,
            "error_rate": self.error_rate,
            "availability": self.availability,
            "latency_p50_ms": self.latency_p50_ms,
            "latency_p95_ms": self.latency_p95_ms,
            "latency_p99_ms": self.latency_p99_ms,
            "saturation": self.saturation,
            "health": self.health.value,
            "sample_count": self.sample_count,
        }


@dataclass(frozen=True, slots=True)
class DashboardOverview:
    """@brief 单个工作区的 Dashboard 聚合结果（dashboard overview）。

    @param scope: 产生结果时使用的工作区范围。
    @param start_at: 查询窗口起点，UTC。
    @param end_at: 查询窗口终点，UTC。
    @param generated_at: 应用层生成摘要的时刻，UTC。
    @param services: 按服务名称排序的服务摘要。
    @param health: 所有服务中最严重的健康状态。
    @param request_count: 所有服务的请求增量总和。
    @param error_count: 所有服务的错误增量总和。
    @param error_rate: 汇总错误率；没有请求时为 None。
    @param availability: 汇总可用性；没有请求时为 None。
    """

    scope: DashboardScope
    start_at: datetime
    end_at: datetime
    generated_at: datetime
    services: tuple[ServiceSummary, ...]
    health: HealthStatus
    request_count: float
    error_count: float
    error_rate: float | None
    availability: float | None

    def to_dict(self) -> dict[str, object]:
        """@brief 转换为 API/CLI/GUI 均可消费的 JSON 结构。

        @return: 使用稳定 snake_case 字段和 RFC 3339 时间的字典。
        """

        return {
            "scope": self.scope.to_dict(),
            "window": {
                "start_at": self.start_at.isoformat().replace("+00:00", "Z"),
                "end_at": self.end_at.isoformat().replace("+00:00", "Z"),
            },
            "generated_at": self.generated_at.isoformat().replace("+00:00", "Z"),
            "health": self.health.value,
            "request_count": self.request_count,
            "error_count": self.error_count,
            "error_rate": self.error_rate,
            "availability": self.availability,
            "services": [service.to_dict() for service in self.services],
        }


__all__ = [
    "DashboardOverview",
    "DashboardScope",
    "HealthPolicy",
    "HealthStatus",
    "MetricKind",
    "MetricQuery",
    "MetricSample",
    "ServiceSummary",
]
