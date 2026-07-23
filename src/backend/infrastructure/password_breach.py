"""@brief 隐私保护的泄露密码检查适配器 / Privacy-preserving breached-password checker adapter.

实现使用 Pwned Passwords range API 的 k-anonymity 协议：只发送候选密码 SHA-1
摘要的前五个十六进制字符，并在本地比较后缀。明文和完整摘要都不会离开进程。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import re
import time
from collections.abc import Callable
from datetime import timedelta

import httpx

#: @brief Pwned Passwords range API 固定端点 / Fixed Pwned Passwords range API endpoint.
_RANGE_ENDPOINT = "https://api.pwnedpasswords.com/range/"
#: @brief SHA-1 后缀的严格十六进制语法 / Strict hexadecimal syntax for a SHA-1 suffix.
_HASH_SUFFIX = re.compile(r"[A-F0-9]{35}\Z", flags=re.ASCII)
#: @brief 单个 range 响应的最大可信字节数 / Maximum accepted byte size of one range response.
_MAX_RESPONSE_BYTES = 1_048_576


class PasswordBreachCheckUnavailable(RuntimeError):
    """@brief 泄露密码服务无法给出可信结论 / Breach service cannot produce a trustworthy decision."""


class PwnedPasswordsChecker:
    """@brief 通过 k-anonymity 查询 Pwned Passwords / Query Pwned Passwords through k-anonymity.

    @param client 受 composition root 管理的异步 HTTP client / Async HTTP client owned by the composition root.
    @param request_timeout 单次远端请求上限 / Per-request remote timeout.
    @param cache_ttl SHA-1 前缀结果的短期缓存寿命 / Short-lived cache lifetime for SHA-1-prefix results.
    @param max_cache_entries 有界缓存条目数 / Bounded cache entry count.
    @param monotonic_clock 可测试的单调时钟 / Testable monotonic clock.
    @note ``Add-Padding`` 降低响应长度侧信道；缓存只保存公开泄露摘要后缀，不保存用户密码。
        / ``Add-Padding`` reduces response-length leakage; the cache stores only public breached-hash
        suffixes, never user passwords.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        request_timeout: float = 3.0,
        cache_ttl: timedelta = timedelta(hours=6),
        max_cache_entries: int = 4096,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """@brief 初始化有界 checker / Initialize the bounded checker.

        @param client 复用连接池的异步 client / Async client with a reusable connection pool.
        @param request_timeout 正数秒级 timeout / Positive timeout in seconds.
        @param cache_ttl 正数缓存寿命 / Positive cache lifetime.
        @param max_cache_entries 正数缓存容量 / Positive cache capacity.
        @param monotonic_clock 单调秒时钟 / Monotonic seconds clock.
        @raise ValueError 配置不安全时抛出 / Raised for unsafe configuration.
        """

        if request_timeout <= 0:
            raise ValueError("breached-password request timeout must be positive")
        if cache_ttl <= timedelta(0):
            raise ValueError("breached-password cache lifetime must be positive")
        if max_cache_entries < 1:
            raise ValueError("breached-password cache capacity must be positive")
        self._client = client
        self._request_timeout = request_timeout
        self._cache_ttl_seconds = cache_ttl.total_seconds()
        self._max_cache_entries = max_cache_entries
        self._clock = monotonic_clock
        self._cache: dict[str, tuple[float, frozenset[str]]] = {}
        self._cache_lock = asyncio.Lock()

    async def is_breached(self, password: str) -> bool:
        """@brief 查询候选密码是否已泄露 / Check whether a candidate password is breached.

        @param password 调用期内的候选明文 / Candidate plaintext scoped to this call.
        @return 完整摘要后缀存在且 count 大于零时为真 / True when the full hash suffix has a
            positive breach count.
        @raise PasswordBreachCheckUnavailable 网络、状态、大小或响应格式不可信时抛出 /
            Raised when transport, status, size, or response syntax is untrustworthy.
        """

        digest = hashlib.sha1(password.encode("utf-8"), usedforsecurity=False).hexdigest().upper()
        prefix, suffix = digest[:5], digest[5:]
        suffixes = await self._suffixes(prefix)
        return any(hmac.compare_digest(suffix, candidate) for candidate in suffixes)

    async def _suffixes(self, prefix: str) -> frozenset[str]:
        """@brief 读取或请求一个摘要前缀集合 / Read or request one digest-prefix set.

        @param prefix 五字符大写 SHA-1 前缀 / Five-character uppercase SHA-1 prefix.
        @return count 大于零的后缀集合 / Suffixes whose breach counts are positive.
        @raise PasswordBreachCheckUnavailable 远端响应不可信时抛出 / Raised for an untrusted
            remote response.
        """

        now = self._clock()
        async with self._cache_lock:
            cached = self._cache.get(prefix)
            if cached is not None and cached[0] > now:
                return cached[1]

        suffixes = await self._fetch_suffixes(prefix)
        expires_at = self._clock() + self._cache_ttl_seconds
        async with self._cache_lock:
            self._cache[prefix] = (expires_at, suffixes)
            self._prune_cache(self._clock())
        return suffixes

    async def _fetch_suffixes(self, prefix: str) -> frozenset[str]:
        """@brief 从固定 HTTPS 端点读取一个 padded range / Fetch one padded range from the fixed HTTPS endpoint.

        @param prefix 五字符大写 SHA-1 前缀 / Five-character uppercase SHA-1 prefix.
        @return count 大于零的后缀集合 / Suffixes with positive counts.
        @raise PasswordBreachCheckUnavailable 请求或解析失败时抛出 / Raised on request or parse failure.
        """

        try:
            response = await self._client.get(
                _RANGE_ENDPOINT + prefix,
                headers={
                    "Add-Padding": "true",
                    "Accept": "text/plain",
                    "User-Agent": "ai-job-workspace-password-safety/1",
                },
                timeout=self._request_timeout,
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise PasswordBreachCheckUnavailable(
                "breached-password range request failed"
            ) from error
        if len(response.content) > _MAX_RESPONSE_BYTES:
            raise PasswordBreachCheckUnavailable(
                "breached-password range response exceeded the byte limit"
            )
        try:
            text = response.content.decode("ascii", errors="strict")
        except UnicodeDecodeError as error:
            raise PasswordBreachCheckUnavailable(
                "breached-password range response was not ASCII"
            ) from error
        return _parse_range_response(text)

    def _prune_cache(self, now: float) -> None:
        """@brief 移除过期和最早插入的缓存项 / Remove expired and oldest cache entries.

        @param now 当前单调秒数 / Current monotonic seconds.
        @note 调用方必须持有 ``_cache_lock`` / Caller must hold ``_cache_lock``.
        """

        expired = [prefix for prefix, item in self._cache.items() if item[0] <= now]
        for prefix in expired:
            self._cache.pop(prefix, None)
        while len(self._cache) > self._max_cache_entries:
            self._cache.pop(next(iter(self._cache)))


def _parse_range_response(payload: str) -> frozenset[str]:
    """@brief 严格解析 Pwned Passwords range 文本 / Strictly parse Pwned Passwords range text.

    @param payload 已解码 ASCII 文本 / Decoded ASCII text.
    @return 正 count 后缀集合 / Positive-count suffix set.
    @raise PasswordBreachCheckUnavailable 空、重复或语法错误响应时抛出 / Raised for an empty,
        duplicate, or syntactically invalid response.
    """

    positive: set[str] = set()
    observed: set[str] = set()
    lines = payload.splitlines()
    if not lines:
        raise PasswordBreachCheckUnavailable("breached-password range response was empty")
    for raw_line in lines:
        suffix, separator, raw_count = raw_line.strip().partition(":")
        if (
            separator != ":"
            or _HASH_SUFFIX.fullmatch(suffix) is None
            or not raw_count.isascii()
            or not raw_count.isdecimal()
            or suffix in observed
        ):
            raise PasswordBreachCheckUnavailable(
                "breached-password range response was malformed"
            )
        observed.add(suffix)
        if int(raw_count) > 0:
            positive.add(suffix)
    return frozenset(positive)


__all__ = ["PasswordBreachCheckUnavailable", "PwnedPasswordsChecker"]
