"""@brief 遥测保留领域策略 / Telemetry-retention domain policy."""

from dataclasses import dataclass
from typing import Final

from .errors import InvalidRetentionPolicyError

DEFAULT_PRUNE_BATCH_SIZE: Final[int] = 1_000
"""@brief 默认单批删除上限 / Default deletion batch limit."""

MAX_PRUNE_BATCH_SIZE: Final[int] = 10_000
"""@brief 单批删除硬上限 / Hard deletion batch limit."""

DEFAULT_PRUNE_MAX_BATCHES: Final[int] = 10
"""@brief 默认最多删除批数 / Default maximum deletion batches."""

MAX_PRUNE_MAX_BATCHES: Final[int] = 100
"""@brief 单次命令最多删除批数 / Hard maximum deletion batches per command."""

DEFAULT_STATEMENT_TIMEOUT_MS: Final[int] = 5_000
"""@brief 默认单批 SQL 超时毫秒数 / Default per-batch SQL timeout in milliseconds."""

MAX_STATEMENT_TIMEOUT_MS: Final[int] = 60_000
"""@brief 单批 SQL 超时硬上限 / Hard per-batch SQL timeout limit."""

DEFAULT_LOCK_TIMEOUT_MS: Final[int] = 500
"""@brief 默认锁等待超时毫秒数 / Default lock-wait timeout in milliseconds."""

MAX_LOCK_TIMEOUT_MS: Final[int] = 5_000
"""@brief 锁等待超时硬上限 / Hard lock-wait timeout limit."""


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """@brief 遥测数据保留边界 / Telemetry data-retention boundary.

    @param days 保留天数；零表示显式禁用清理 / Retention days; zero explicitly disables pruning.
    """

    days: int

    def __post_init__(self) -> None:
        """@brief 校验保留天数 / Validate retention days.

        @return 无返回值 / No return value.
        @raise InvalidRetentionPolicyError 天数不是非负整数时抛出。
        / Raised when days is not a non-negative integer.
        """
        if not isinstance(self.days, int) or isinstance(self.days, bool) or self.days < 0:
            raise InvalidRetentionPolicyError("retention days 必须是非负整数。")

    @property
    def enabled(self) -> bool:
        """@brief 判断自动清理是否启用 / Report whether pruning is enabled.

        @return 天数大于零时为真 / True when days is greater than zero.
        """
        return self.days > 0


@dataclass(frozen=True, slots=True)
class PruneLimits:
    """@brief 单次遥测清理的资源护栏 / Resource guardrails for one telemetry prune.

    @param batch_size 每批最多删除记录数 / Maximum rows deleted per batch.
    @param max_batches 单次运行最多批数 / Maximum batches in one run.
    @param statement_timeout_ms 单批语句超时毫秒数 / Per-batch statement timeout in milliseconds.
    @param lock_timeout_ms 单批锁等待超时毫秒数 / Per-batch lock timeout in milliseconds.
    """

    batch_size: int = DEFAULT_PRUNE_BATCH_SIZE
    max_batches: int = DEFAULT_PRUNE_MAX_BATCHES
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS
    lock_timeout_ms: int = DEFAULT_LOCK_TIMEOUT_MS

    def __post_init__(self) -> None:
        """@brief 校验所有有界运维参数 / Validate all bounded operator controls.

        @return 无返回值 / No return value.
        @raise InvalidRetentionPolicyError 任一边界不是范围内整数时抛出。
        / Raised when any bound is not an integer in its allowed range.
        """
        self._require_bounded("batch_size", self.batch_size, 1, MAX_PRUNE_BATCH_SIZE)
        self._require_bounded("max_batches", self.max_batches, 1, MAX_PRUNE_MAX_BATCHES)
        self._require_bounded(
            "statement_timeout_ms", self.statement_timeout_ms, 1, MAX_STATEMENT_TIMEOUT_MS
        )
        self._require_bounded("lock_timeout_ms", self.lock_timeout_ms, 1, MAX_LOCK_TIMEOUT_MS)
        if self.lock_timeout_ms > self.statement_timeout_ms:
            raise InvalidRetentionPolicyError("lock_timeout_ms 不能大于 statement_timeout_ms。")

    @staticmethod
    def _require_bounded(label: str, value: int, minimum: int, maximum: int) -> None:
        """@brief 校验一个闭区间整数 / Validate an integer in a closed interval.

        @param label 运维参数名 / Operator-control name.
        @param value 待校验值 / Candidate value.
        @param minimum 允许的最小值 / Inclusive minimum.
        @param maximum 允许的最大值 / Inclusive maximum.
        @return 无返回值 / No return value.
        """
        if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
            raise InvalidRetentionPolicyError(f"{label} 必须是 {minimum} 到 {maximum} 的整数。")
