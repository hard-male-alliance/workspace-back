"""@brief 遥测保留清理应用用例 / Telemetry-retention pruning application use case."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Literal

from dbctl.domain.retention import PruneLimits, RetentionPolicy

from .errors import RetentionExecutionError
from .ports import TelemetryRetentionPort


class PruneMode(StrEnum):
    """@brief 遥测清理的显式执行模式 / Explicit telemetry-pruning execution mode."""

    DRY_RUN = "dry-run"
    APPLY = "apply"


@dataclass(frozen=True, slots=True)
class PruneRequest:
    """@brief 一次受限遥测清理请求 / One bounded telemetry-pruning request.

    @param policy 领域保留策略 / Domain retention policy.
    @param limits 删除与锁资源护栏 / Deletion and lock resource guardrails.
    @param mode 不连接的 dry-run 或显式 apply / Disconnected dry-run or explicit apply.
    """

    policy: RetentionPolicy
    limits: PruneLimits
    mode: PruneMode = PruneMode.DRY_RUN

    def __post_init__(self) -> None:
        """@brief 校验请求只含强类型策略 / Require strongly typed request components.

        @return 无返回值 / No return value.
        """
        if not isinstance(self.policy, RetentionPolicy):
            raise RetentionExecutionError("prune policy 必须是 RetentionPolicy。")
        if not isinstance(self.limits, PruneLimits):
            raise RetentionExecutionError("prune limits 必须是 PruneLimits。")
        if not isinstance(self.mode, PruneMode):
            raise RetentionExecutionError("prune mode 必须是 PruneMode。")


@dataclass(frozen=True, slots=True)
class DeleteTelemetryBatch:
    """@brief 删除一个短事务批次的命令 / Command to delete one short-transaction batch.

    @param cutoff 仅删除早于该 UTC 时刻的记录 / Delete only records older than this UTC instant.
    @param limits 端口必须遵守的资源边界 / Resource bounds the port must honor.
    """

    cutoff: datetime
    limits: PruneLimits

    def __post_init__(self) -> None:
        """@brief 校验批次命令 / Validate the batch command.

        @return 无返回值 / No return value.
        """
        _require_aware_datetime(self.cutoff, label="delete cutoff")
        if not isinstance(self.limits, PruneLimits):
            raise RetentionExecutionError("delete limits 必须是 PruneLimits。")


@dataclass(frozen=True, slots=True)
class StaleTelemetryProbe:
    """@brief 探测过期记录的短事务查询 / Short-transaction query for stale records.

    @param cutoff 过期边界 UTC 时刻 / UTC staleness cutoff.
    @param limits 查询超时与锁等待护栏 / Query timeout and lock-wait guardrails.
    """

    cutoff: datetime
    limits: PruneLimits

    def __post_init__(self) -> None:
        """@brief 校验探测命令 / Validate the probe command.

        @return 无返回值 / No return value.
        """
        _require_aware_datetime(self.cutoff, label="probe cutoff")
        if not isinstance(self.limits, PruneLimits):
            raise RetentionExecutionError("probe limits 必须是 PruneLimits。")


@dataclass(frozen=True, slots=True)
class RetentionDisabled:
    """@brief retention_days 为零的停用结果 / Disabled outcome for zero retention days.

    @param policy 显式停用的保留策略 / Explicitly disabled retention policy.
    @param limits 未使用但供安全摘要展示的边界 / Unused limits retained for safe summaries.
    """

    policy: RetentionPolicy
    limits: PruneLimits
    kind: Literal["retention_disabled"] = field(init=False, default="retention_disabled")

    def __post_init__(self) -> None:
        """@brief 保证该分支只能表示停用策略 / Ensure this branch represents only disabled policy.

        @return 无返回值 / No return value.
        """
        if not isinstance(self.policy, RetentionPolicy) or self.policy.enabled:
            raise RetentionExecutionError("RetentionDisabled 只能表示 days=0。")
        if not isinstance(self.limits, PruneLimits):
            raise RetentionExecutionError("RetentionDisabled.limits 无效。")


@dataclass(frozen=True, slots=True)
class PrunePreview:
    """@brief 不连接数据库的 dry-run 结果 / Disconnected dry-run outcome.

    @param policy 已启用保留策略 / Enabled retention policy.
    @param limits 预览的资源护栏 / Previewed resource guardrails.
    @param cutoff 计划采用的 UTC 删除边界 / Planned UTC deletion cutoff.
    """

    policy: RetentionPolicy
    limits: PruneLimits
    cutoff: datetime
    kind: Literal["prune_preview"] = field(init=False, default="prune_preview")

    def __post_init__(self) -> None:
        """@brief 校验 dry-run 结果 / Validate the dry-run outcome.

        @return 无返回值 / No return value.
        """
        if not isinstance(self.policy, RetentionPolicy) or not self.policy.enabled:
            raise RetentionExecutionError("PrunePreview 需要已启用的 retention policy。")
        if not isinstance(self.limits, PruneLimits):
            raise RetentionExecutionError("PrunePreview.limits 无效。")
        _require_aware_datetime(self.cutoff, label="preview cutoff")


@dataclass(frozen=True, slots=True)
class PruneApplied:
    """@brief 已执行有界删除的结果 / Outcome of applied bounded deletion.

    @param policy 已执行的保留策略 / Applied retention policy.
    @param limits 已执行的资源护栏 / Applied resource guardrails.
    @param cutoff 本轮固定 UTC 删除边界 / Fixed UTC cutoff for this run.
    @param batch_count 已提交的删除批次数 / Committed deletion-batch count.
    @param deleted_count 已删除总记录数 / Total deleted records.
    @param has_more 是否仍有早于 cutoff 的记录 / Whether records older than cutoff remain.
    """

    policy: RetentionPolicy
    limits: PruneLimits
    cutoff: datetime
    batch_count: int
    deleted_count: int
    has_more: bool
    kind: Literal["prune_applied"] = field(init=False, default="prune_applied")

    def __post_init__(self) -> None:
        """@brief 校验已执行结果的有界性 / Validate boundedness of the applied outcome.

        @return 无返回值 / No return value.
        """
        if not isinstance(self.policy, RetentionPolicy) or not self.policy.enabled:
            raise RetentionExecutionError("PruneApplied 需要已启用的 retention policy。")
        if not isinstance(self.limits, PruneLimits):
            raise RetentionExecutionError("PruneApplied.limits 无效。")
        _require_aware_datetime(self.cutoff, label="applied cutoff")
        if (
            not isinstance(self.batch_count, int)
            or isinstance(self.batch_count, bool)
            or not 1 <= self.batch_count <= self.limits.max_batches
        ):
            raise RetentionExecutionError("PruneApplied.batch_count 违反批次边界。")
        if (
            not isinstance(self.deleted_count, int)
            or isinstance(self.deleted_count, bool)
            or not 0 <= self.deleted_count <= self.batch_count * self.limits.batch_size
        ):
            raise RetentionExecutionError("PruneApplied.deleted_count 违反删除边界。")
        if not isinstance(self.has_more, bool):
            raise RetentionExecutionError("PruneApplied.has_more 必须是布尔值。")

    @property
    def reached_batch_limit(self) -> bool:
        """@brief 判断本轮是否耗尽批次预算 / Report whether this run exhausted its batch budget.

        @return batch_count 达到 max_batches 时为真 / True when batch_count reached max_batches.
        """
        return self.batch_count >= self.limits.max_batches


type PruneOutcome = RetentionDisabled | PrunePreview | PruneApplied
"""@brief 以 kind 字面量判别的遥测清理结果 / Kind-discriminated telemetry-prune outcome."""


def _utc_now() -> datetime:
    """@brief 返回当前 UTC 时间 / Return current UTC time.

    @return 带 UTC 时区的 datetime / UTC-aware datetime.
    """
    return datetime.now(UTC)


class PruneTelemetryService:
    """@brief 编排停用、预览与短事务批量删除 / Orchestrate disabled, preview, and short batches."""

    def __init__(
        self,
        port: TelemetryRetentionPort | None,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        """@brief 初始化遥测清理用例 / Initialize the telemetry-pruning use case.

        @param port apply 模式使用的 I/O 端口；其他分支可为 None。
        / I/O port for apply mode; other branches permit None.
        @param clock 可注入的带时区时钟 / Injectable timezone-aware clock.
        """
        self._port = port
        self._clock = clock

    def execute(self, request: PruneRequest) -> PruneOutcome:
        """@brief 执行或预览一轮遥测清理 / Apply or preview one telemetry-pruning run.

        @param request 强类型保留策略、护栏与模式 / Typed retention policy, guardrails, and mode.
        @return 可穷尽模式匹配的判别联合 / Discriminated union for exhaustive matching.
        """
        if not isinstance(request, PruneRequest):
            raise RetentionExecutionError("prune use case 需要 PruneRequest。")
        if not request.policy.enabled:
            return RetentionDisabled(policy=request.policy, limits=request.limits)

        cutoff = _cutoff(self._clock(), request.policy)
        if request.mode is PruneMode.DRY_RUN:
            return PrunePreview(policy=request.policy, limits=request.limits, cutoff=cutoff)
        if self._port is None:
            raise RetentionExecutionError("apply 模式缺少 TelemetryRetentionPort。")

        batch_count = 0
        deleted_count = 0
        command = DeleteTelemetryBatch(cutoff=cutoff, limits=request.limits)
        for _batch_number in range(request.limits.max_batches):
            deleted_in_batch = self._port.delete_batch(command)
            if (
                not isinstance(deleted_in_batch, int)
                or isinstance(deleted_in_batch, bool)
                or not 0 <= deleted_in_batch <= request.limits.batch_size
            ):
                raise RetentionExecutionError("遥测删除端口违反单批删除边界。")
            batch_count += 1
            deleted_count += deleted_in_batch
            if deleted_in_batch < request.limits.batch_size:
                break

        has_more = self._port.has_stale(StaleTelemetryProbe(cutoff=cutoff, limits=request.limits))
        if not isinstance(has_more, bool):
            raise RetentionExecutionError("遥测删除端口返回了无效剩余状态。")
        return PruneApplied(
            policy=request.policy,
            limits=request.limits,
            cutoff=cutoff,
            batch_count=batch_count,
            deleted_count=deleted_count,
            has_more=has_more,
        )


def _cutoff(now: datetime, policy: RetentionPolicy) -> datetime:
    """@brief 计算本轮固定 UTC cutoff / Compute the fixed UTC cutoff for one run.

    @param now 时钟返回的当前时间 / Current time returned by the clock.
    @param policy 已启用保留策略 / Enabled retention policy.
    @return 转换为 UTC 后减去保留天数的时刻 / UTC instant minus retention days.
    """
    _require_aware_datetime(now, label="prune clock")
    try:
        return now.astimezone(UTC) - timedelta(days=policy.days)
    except (OverflowError, ValueError) as error:
        raise RetentionExecutionError("retention days 超出 datetime 可表示范围。") from error


def _require_aware_datetime(value: datetime, *, label: str) -> None:
    """@brief 要求带时区 datetime / Require a timezone-aware datetime.

    @param value 待验证时间 / Candidate time.
    @param label 安全诊断标签 / Safe diagnostic label.
    @return 无返回值 / No return value.
    """
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise RetentionExecutionError(f"{label} 必须是带时区的 datetime。")
