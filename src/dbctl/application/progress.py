"""@brief dbctl 应用操作进度契约 / Application-operation progress contract for dbctl."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


class OperationName(StrEnum):
    """@brief 运维用例的稳定名称 / Stable names of database-operation use cases."""

    CONFIGURATION = "configuration"
    BOOTSTRAP = "bootstrap"
    MIGRATION = "migration"
    PRUNE_TELEMETRY = "prune-telemetry"
    SHELL = "shell"


class ProgressState(StrEnum):
    """@brief 一条操作进度的生命周期状态 / Lifecycle state of one progress update."""

    STARTED = "started"
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ProgressUpdate:
    """@brief 不含秘密的同步操作进度 / Secret-free synchronous operation progress.

    @param operation 产生消息的运维用例 / Database-operation use case producing the update.
    @param state 当前动作的生命周期状态 / Lifecycle state of the current action.
    @param message 面向操作者的简短动作描述 / Concise operator-facing action description.
    @param detail 可选的安全上下文或影响说明 / Optional safe context or impact statement.
    @param current 有界步骤的从一开始序号 / One-based position of a bounded step.
    @param total 有界步骤总数 / Total number of bounded steps.
    @note 这是应用用例的输出契约，不是领域事件，也不是日志记录。
    / This is an application-use-case output contract, not a domain event or log record.
    """

    operation: OperationName
    state: ProgressState
    message: str
    detail: str | None = None
    current: int | None = None
    total: int | None = None

    def __post_init__(self) -> None:
        """@brief 拒绝含糊或不完整的进度状态 / Reject ambiguous or incomplete progress states.

        @return 无返回值 / No return value.
        @raise ValueError 字段类型、文本或步骤边界无效时抛出。
        / Raised when field types, text, or step bounds are invalid.
        """

        if not isinstance(self.operation, OperationName):
            raise ValueError("progress operation 必须是 OperationName。")
        if not isinstance(self.state, ProgressState):
            raise ValueError("progress state 必须是 ProgressState。")
        if not isinstance(self.message, str) or not self.message.strip():
            raise ValueError("progress message 不能为空。")
        if self.detail is not None and (
            not isinstance(self.detail, str) or not self.detail.strip()
        ):
            raise ValueError("progress detail 必须是非空文本或 None。")
        if (self.current is None) != (self.total is None):
            raise ValueError("progress current 与 total 必须同时存在或同时缺省。")
        if self.current is not None and self.total is not None:
            if (
                not isinstance(self.current, int)
                or isinstance(self.current, bool)
                or not isinstance(self.total, int)
                or isinstance(self.total, bool)
                or not 1 <= self.current <= self.total
            ):
                raise ValueError("progress 步骤必须满足 1 <= current <= total。")
        object.__setattr__(self, "message", self.message.strip())
        if self.detail is not None:
            object.__setattr__(self, "detail", self.detail.strip())


class ProgressSink(Protocol):
    """@brief 同步消费应用进度的单一输出端口 / Single output port consuming progress synchronously."""

    def publish(self, update: ProgressUpdate) -> None:
        """@brief 发布一条已验证进度 / Publish one validated progress update.

        @param update 不含 secret 的操作进度 / Secret-free operation progress.
        @return 无返回值 / No return value.
        """

        ...


def publish_progress(sink: ProgressSink | None, update: ProgressUpdate) -> None:
    """@brief 尽力发布且不让呈现故障改变业务语义 / Publish best-effort without changing business semantics.

    @param sink 可选进度消费者 / Optional progress consumer.
    @param update 已验证且不含 secret 的进度 / Validated secret-free progress update.
    @return 无返回值 / No return value.
    @note stderr 关闭、终端断开或自定义 sink 缺陷不得把已提交的数据库操作报告为失败，
    否则会诱发危险重试。/ A closed stderr, detached terminal, or faulty custom sink must not turn
    a committed database operation into a reported failure and induce an unsafe retry.
    """

    if sink is None:
        return
    try:
        sink.publish(update)
    except Exception:
        return


__all__ = [
    "OperationName",
    "ProgressSink",
    "ProgressState",
    "ProgressUpdate",
    "publish_progress",
]
