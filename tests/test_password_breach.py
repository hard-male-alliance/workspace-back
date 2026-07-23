"""@brief 泄露密码检查的隐私与 fail-closed 回归 / Privacy and fail-closed tests for breached-password checks."""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable

import httpx
import pytest

from backend.application.identity import _validate_new_password
from backend.domain.identity import HostedIdentityError
from backend.infrastructure.password_breach import (
    PasswordBreachCheckUnavailable,
    PwnedPasswordsChecker,
)

_CANDIDATE = "correct horse battery staple!"
"""@brief 测试候选密码 / Test candidate password."""


def _digest_parts(password: str) -> tuple[str, str]:
    """@brief 返回测试密码的 SHA-1 前缀/后缀 / Return SHA-1 prefix/suffix for a test password.

    @param password 测试输入 / Test input.
    @return 五字符前缀与三十五字符后缀 / Five-character prefix and 35-character suffix.
    """

    digest = hashlib.sha1(password.encode(), usedforsecurity=False).hexdigest().upper()
    return digest[:5], digest[5:]


@pytest.mark.asyncio
async def test_pwned_passwords_uses_only_a_padded_prefix_and_caches_public_suffixes() -> None:
    """@brief 远端只看到 padded 前缀且相同前缀命中缓存 / Remote sees only a padded prefix and repeated prefixes hit cache."""

    prefix, suffix = _digest_parts(_CANDIDATE)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        """@brief 捕获 range 请求并返回 padded 风格语料 / Capture range request and return padding-style corpus.

        @param request range 请求 / Range request.
        @return 两个后缀行 / Two suffix rows.
        """

        requests.append(request)
        return httpx.Response(
            200,
            text=f"{suffix}:42\r\n{'0' * 35}:0\r\n",
            headers={"Content-Type": "text/plain"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        checker = PwnedPasswordsChecker(client)
        assert await checker.is_breached(_CANDIDATE) is True
        assert await checker.is_breached(_CANDIDATE) is True

    assert len(requests) == 1
    request = requests[0]
    assert request.url == f"https://api.pwnedpasswords.com/range/{prefix}"
    assert request.headers["add-padding"] == "true"
    assert _CANDIDATE not in str(request.url)
    assert suffix not in str(request.url)


@pytest.mark.asyncio
async def test_pwned_passwords_returns_false_when_the_suffix_is_absent() -> None:
    """@brief 相同前缀下无完整后缀匹配时返回 false / Return false without a full-suffix match."""

    def handler(request: httpx.Request) -> httpx.Response:
        """@brief 返回不匹配后缀 / Return a non-matching suffix.

        @param request 未使用的 range 请求 / Unused range request.
        @return 合法响应 / Valid response.
        """

        del request
        return httpx.Response(200, text=f"{'A' * 35}:7\n{'B' * 35}:0\n")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await PwnedPasswordsChecker(client).is_breached(_CANDIDATE) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handler",
    [
        lambda request: httpx.Response(503),
        lambda request: httpx.Response(200, text="not-a-range-row"),
        lambda request: httpx.Response(200, content=b"\xff"),
        lambda request: httpx.Response(200, content=b"A" * 1_048_577),
    ],
)
async def test_pwned_passwords_fails_closed_for_untrusted_responses(
    handler: Callable[[httpx.Request], httpx.Response],
) -> None:
    """@brief 网络状态、语法、编码和大小异常均不放行 / Status, syntax, encoding, and size failures all fail closed.

    @param handler 构造异常响应的 transport handler / Transport handler producing an invalid response.
    """

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PasswordBreachCheckUnavailable):
            await PwnedPasswordsChecker(client).is_breached(_CANDIDATE)


class _Checker:
    """@brief 可控应用端口替身 / Controllable application-port test double."""

    def __init__(self, result: bool | RuntimeError) -> None:
        """@brief 保存结果 / Store the result.

        @param result 布尔结论或失败 / Boolean decision or failure.
        """

        self._result = result

    async def is_breached(self, password: str) -> bool:
        """@brief 返回配置结论 / Return the configured decision.

        @param password 未保留的候选密码 / Candidate password that is not retained.
        @return 配置布尔值 / Configured boolean.
        @raise RuntimeError 配置为失败时抛出 / Raised when configured as a failure.
        """

        del password
        if isinstance(self._result, RuntimeError):
            raise self._result
        return self._result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("checker", "expected_code", "expected_status"),
    [
        (_Checker(True), "identity.password_breached", 400),
        (
            _Checker(RuntimeError("remote unavailable")),
            "identity.password_safety_unavailable",
            503,
        ),
    ],
)
async def test_identity_password_policy_rejects_breached_and_unverifiable_passwords(
    checker: _Checker,
    expected_code: str,
    expected_status: int,
) -> None:
    """@brief 应用层把泄露与不可验证映射为稳定安全错误 / Application maps breach and unavailable checks to stable safe errors.

    @param checker 可控检查器 / Controllable checker.
    @param expected_code 预期稳定错误码 / Expected stable error code.
    @param expected_status 预期 HTTP 状态投影 / Expected HTTP status projection.
    """

    with pytest.raises(HostedIdentityError) as raised:
        await _validate_new_password(_CANDIDATE, checker)
    assert raised.value.code == expected_code
    assert raised.value.status == expected_status


@pytest.mark.asyncio
async def test_identity_password_policy_accepts_a_long_password_absent_from_the_corpus() -> None:
    """@brief 满足长度且不在语料的密码通过 / A sufficiently long password absent from the corpus passes."""

    result: Awaitable[None] = _validate_new_password(_CANDIDATE, _Checker(False))
    await result
