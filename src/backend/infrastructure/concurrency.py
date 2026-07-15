"""@brief 结构化并发与反压基础设施 / Structured-concurrency and backpressure infrastructure."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

type WorkOperation = Callable[[], Awaitable[None]]
type FailureHandler = Callable[[BaseException], Awaitable[None]]


class BackpressureError(RuntimeError):
    """@brief 有界队列已满 / A bounded queue is full."""


@dataclass(frozen=True, slots=True)
class WorkLimits:
    """@brief 一类外部工作的并发限制 / Concurrency limit for one external-work class."""

    name: str
    concurrency: int


class BoundedTaskSupervisor:
    """@brief 由 lifespan 拥有的有界 TaskGroup / Lifespan-owned bounded TaskGroup.

    @note 这里是唯一创建后台 task 的路径；每项外部工作都有类别上限和全局排队上限。
    """

    def __init__(self, limits: tuple[WorkLimits, ...], queue_capacity: int, shutdown_grace_ms: int) -> None:
        """@brief 初始化受控任务监督器 / Initialize the controlled task supervisor.

        @param limits 各工作类别限制 / Limits per work class.
        @param queue_capacity 等待或运行任务的最大数量 / Maximum queued or running tasks.
        @param shutdown_grace_ms 优雅关闭上限 / Graceful shutdown bound.
        """
        if queue_capacity <= 0 or shutdown_grace_ms <= 0 or any(limit.concurrency <= 0 for limit in limits):
            raise ValueError("work limits and capacities must be positive")
        self._limits = {limit.name: asyncio.Semaphore(limit.concurrency) for limit in limits}
        self._capacity = queue_capacity
        self._shutdown_grace_seconds = shutdown_grace_ms / 1000
        self._group: asyncio.TaskGroup | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._accepting = False

    @property
    def saturation(self) -> float:
        """@brief 返回任务池当前饱和度 / Return the current task-pool saturation.

        @return 已提交任务占全局容量的比例，范围 [0, 1] / Submitted-task fraction of global capacity in [0, 1].
        """
        return min(1.0, len(self._tasks) / self._capacity)

    async def __aenter__(self) -> BoundedTaskSupervisor:
        """@brief 进入 TaskGroup 所有权范围 / Enter TaskGroup ownership scope.

        @return 可提交任务的监督器 / Supervisor accepting work.
        """
        self._group = asyncio.TaskGroup()
        await self._group.__aenter__()
        self._accepting = True
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """@brief 停止接单并有界关闭 / Stop accepting work and close within a bound.

        @param exc_type 上下文异常类型 / Context exception type.
        @param exc 上下文异常 / Context exception.
        @param traceback 上下文 traceback / Context traceback.
        """
        self._accepting = False
        pending = set(self._tasks)
        if pending:
            _, pending = await asyncio.wait(pending, timeout=self._shutdown_grace_seconds)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if self._group is not None:
            await self._group.__aexit__(None, None, None)
            self._group = None

    def submit(
        self,
        work_class: str,
        operation: WorkOperation,
        on_failure: FailureHandler | None = None,
        name: str | None = None,
    ) -> asyncio.Task[None]:
        """@brief 向受控 TaskGroup 提交工作 / Submit work to the controlled TaskGroup.

        @param work_class 外部工作类别 / External-work class.
        @param operation 可取消协程工厂 / Cancellable coroutine factory.
        @param on_failure 非取消失败回调 / Non-cancellation failure callback.
        @param name 诊断任务名 / Diagnostic task name.
        @return 受监督的 task / Supervised task.
        @raise BackpressureError 容量已满时抛出 / Raised when capacity is exhausted.
        @raise RuntimeError 生命周期未启动或已关闭时抛出 / Raised outside the active lifecycle.
        """
        if not self._accepting or self._group is None:
            raise RuntimeError("task supervisor is not accepting work")
        if work_class not in self._limits:
            raise ValueError(f"unknown work class: {work_class}")
        if len(self._tasks) >= self._capacity:
            raise BackpressureError("bounded work queue is full")
        task = self._group.create_task(
            self._run(work_class, operation, on_failure),
            name=name or f"aiws:{work_class}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _run(
        self,
        work_class: str,
        operation: WorkOperation,
        on_failure: FailureHandler | None,
    ) -> None:
        """@brief 在类别 semaphore 中执行工作 / Run work within its class semaphore.

        @param work_class 外部工作类别 / External-work class.
        @param operation 可取消协程工厂 / Cancellable coroutine factory.
        @param on_failure 失败处理器 / Failure handler.
        """
        try:
            async with self._limits[work_class]:
                await operation()
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            if on_failure is not None:
                await on_failure(error)
