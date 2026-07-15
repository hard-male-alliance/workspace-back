"""@brief HTTP 命令的幂等性注册表 / Idempotency registry for HTTP commands."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from backend.domain.common import DomainError, Problem
from workspace_shared.tenancy import ActorScope


@dataclass(frozen=True, slots=True)
class IdempotentResponse:
    """@brief 可重放的 HTTP 响应 / Replayable HTTP response."""

    status_code: int
    body: dict[str, Any]


class IdempotencyRegistry:
    """@brief actor + path 作用域的内存幂等注册表 / In-memory idempotency registry scoped by actor and path.

    @note MOCK — PostgreSQL 实现应持久化 body hash 与完整响应，且设置保留期。全局
    锁只保护索引；实际命令按完整 tenant/actor/target/key 分片，避免一个慢命令阻塞
    其它工作区。
    """

    def __init__(self) -> None:
        """@brief 初始化注册表 / Initialize the registry."""
        self._index_lock = asyncio.Lock()
        self._entries: dict[tuple[str, str, str, str, str], tuple[str, IdempotentResponse]] = {}
        self._key_locks: dict[tuple[str, str, str, str, str], asyncio.Lock] = {}

    async def execute(
        self,
        scope: ActorScope,
        path: str,
        key: str,
        payload: object,
        status_code: int,
        operation: Callable[[], Awaitable[dict[str, Any]]],
    ) -> IdempotentResponse:
        """@brief 执行或回放幂等命令 / Execute or replay an idempotent command.

        @param scope 多租户范围 / Multi-tenant scope.
        @param path HTTP 路径模板 / HTTP path template.
        @param key Idempotency-Key / Idempotency-Key.
        @param payload 请求 payload / Request payload.
        @param status_code 首次成功状态码 / First-success status code.
        @param operation 首次执行协程 / First-execution coroutine.
        @return 首次或已缓存响应 / First or cached response.
        @raise DomainError 同 key 的 body 不一致时抛出 / Raised when the same key has a different body.
        """
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()
        registry_key = (
            scope.actor_id,
            scope.workspace_id,
            scope.resource_owner_id,
            path,
            key,
        )
        async with self._index_lock:
            existing = self._entries.get(registry_key)
            if existing is not None:
                if existing[0] != digest:
                    raise DomainError(Problem("idempotency.key_reused", 409, "Idempotency key was reused with different input"))
                return IdempotentResponse(existing[1].status_code, deepcopy(existing[1].body))
            key_lock = self._key_locks.setdefault(registry_key, asyncio.Lock())

        async with key_lock:
            async with self._index_lock:
                existing = self._entries.get(registry_key)
            if existing is not None:
                if existing[0] != digest:
                    raise DomainError(Problem("idempotency.key_reused", 409, "Idempotency key was reused with different input"))
                return IdempotentResponse(existing[1].status_code, deepcopy(existing[1].body))
            body = await operation()
            response = IdempotentResponse(status_code, deepcopy(body))
            async with self._index_lock:
                self._entries[registry_key] = (digest, response)
            return response
