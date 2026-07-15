"""@brief OpenAI 兼容模型 Provider 的无网络测试 / Network-free tests for the OpenAI-compatible model provider."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from backend.infrastructure.providers import (
    FallbackModelProvider,
    MockModelProvider,
    ModelProviderStreamError,
    OpenAICompatibleModelProvider,
    ProviderRateLimiter,
)


def _external_inference(*, allow_fallback: bool = True, region: str = "global") -> dict[str, object]:
    """@brief 构建允许外部模型的正式推理意图 / Build a formal inference intent permitting external models.

    @param allow_fallback 是否同意将 prompt 发送给备用 provider / Whether fallback may receive the prompt.
    @param region 允许的实际数据处理地域 / Allowed actual data-processing region.
    @return 完整 inference 对象。
    """
    return {
        "quality_tier": "balanced",
        "latency_budget_ms": 15_000,
        "cost_tier": "standard",
        "data_region": region,
        "allow_provider_fallback": allow_fallback,
        "allow_external_model_processing": True,
    }


def test_openai_compatible_provider_uses_only_the_explicit_shared_proxy() -> None:
    """@brief provider 仅接收配置的统一出站代理 / Provider accepts only the configured shared outbound proxy.

    @note 该测试替换 HTTPX（HTTP client）构造器，因此不访问网络也不会依赖私有 transport
    internals；``trust_env=False`` 防止环境变量在未审计时劫持 provider 的出站流量。
    """
    captured: dict[str, object] = {}

    class CapturingClient:
        """@brief 记录 HTTPX client 构造参数的替身 / Stand-in capturing HTTPX client constructor parameters."""

        def __init__(self, **kwargs: object) -> None:
            """@brief 保存构造参数 / Save constructor arguments.

            @param kwargs 应传给 HTTPX 的 keyword 参数 / Keyword arguments intended for HTTPX.
            @return 无返回值。
            """
            captured.update(kwargs)

    with patch("backend.infrastructure.providers.httpx.AsyncClient", CapturingClient):
        _ = OpenAICompatibleModelProvider(
            provider="openrouter",
            model="server-side-model",
            base_url="https://provider.example/v1",
            api_key="test-only-secret",
            outbound_proxy_url="http://proxy.internal:8080",
        )

    assert captured["proxy"] == "http://proxy.internal:8080"
    assert captured["trust_env"] is False


@pytest.mark.asyncio
async def test_openai_compatible_provider_streams_delta_and_builds_safe_payload() -> None:
    """@brief Provider 应将能力和内部历史编入请求，并只输出 ``delta.content`` / Provider should encode capability/history and emit only ``delta.content``.

    @note ``MockTransport`` 证明测试不会向真实 provider 或 API key 发起网络请求。
    """
    observed_request: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        """@brief 捕获客户端请求并返回最小 SSE / Capture the client request and return minimal SSE.

        @param request MockTransport 收到的请求 / Request received by MockTransport.
        @return 模拟的成功 SSE 响应 / Simulated successful SSE response.
        """
        observed_request["url"] = str(request.url)
        observed_request["authorization"] = request.headers.get("authorization")
        observed_request["payload"] = json.loads(request.content)
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream; charset=utf-8"},
            content=(
                b'data: {"choices":[{"delta":{"role":"assistant"},"finish_reason":null}]}\n\n'
                b'data: {"choices":[{"delta":{"content":"\xe4\xbd\xa0\xe5\xa5\xbd"},"finish_reason":null}]}\n\n'
                b'data: {"choices":[{"delta":{"content":"\xef\xbc\x8c\xe5\x90\x8c\xe5\xad\xa6"},"finish_reason":"stop"}]}\n\n'
                b"data: [DONE]\n\n"
            ),
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        provider = OpenAICompatibleModelProvider(
            provider="openrouter",
            model="openai/gpt-5-mini",
            base_url="https://openrouter.ai/api/v1",
            api_key="test-only-secret",
            client=client,
        )
        chunks = [
            chunk
            async for chunk in provider.stream_text(
                "请帮我润色项目经历。",
                {
                    "capability": "resume_edit",
                    "response_locale": "zh-CN",
                    "output_modes": ["text"],
                    "messages": [{"role": "assistant", "content": "上一轮建议"}],
                    "inference": _external_inference(),
                },
            )
        ]
        await provider.aclose()
        assert not client.is_closed

    assert chunks == ["你好", "，同学"]
    assert observed_request["url"] == "https://openrouter.ai/api/v1/chat/completions"
    assert observed_request["authorization"] == "Bearer test-only-secret"
    payload = observed_request["payload"]
    assert payload["model"] == "openai/gpt-5-mini"
    assert payload["stream"] is True
    assert payload["messages"][0]["role"] == "system"
    assert "resume wording" in payload["messages"][0]["content"]
    assert payload["messages"][1] == {"role": "assistant", "content": "上一轮建议"}
    assert payload["messages"][-1] == {"role": "user", "content": "请帮我润色项目经历。"}


@pytest.mark.asyncio
async def test_openai_compatible_provider_redacts_invalid_sse() -> None:
    """@brief 非法 SSE 不应泄露上游内容，且应成为受控错误 / Invalid SSE must not leak upstream content and must become a controlled error."""

    def handler(request: httpx.Request) -> httpx.Response:
        """@brief 返回故意损坏的 SSE 帧 / Return an intentionally malformed SSE frame.

        @param request MockTransport 请求 / MockTransport request.
        @return 无效 SSE 响应 / Invalid SSE response.
        """
        del request
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=b"data: this is not JSON and must remain private\n\n",
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = OpenAICompatibleModelProvider(
            provider="china-compatible",
            model="server-side-model",
            base_url="https://provider.example/v1",
            api_key="never-expose-this-key",
            client=client,
        )
        with pytest.raises(ModelProviderStreamError) as error:
            _ = [
                chunk
                async for chunk in provider.stream_text(
                    "测试", {"capability": "general", "inference": _external_inference()}
                )
            ]

    assert error.value.problem.code == "agent.provider_protocol_error"
    assert "private" not in str(error.value)
    assert "never-expose-this-key" not in str(error.value)


@pytest.mark.asyncio
async def test_provider_rate_limit_rejects_a_second_http_request_within_its_bounded_wait() -> None:
    """@brief endpoint 限流器应在本地窗口已满时拒绝第二个真实 HTTP 请求 / Endpoint limiter should reject a second actual HTTP request when its local window is full.

    @note ``MockTransport`` 证明限流发生在 provider HTTP 边界且测试不会等待真实的一分钟
    窗口；第二个流在配置的短等待后返回稳定、可重试的 429 问题。
    """
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        """@brief 记录 HTTP 调用并返回完成 SSE / Count HTTP calls and return a completed SSE.

        @param request MockTransport 收到的请求。
        @return 最小有效 SSE 响应。
        """
        nonlocal calls
        del request
        calls += 1
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=(
                b'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n'
                b"data: [DONE]\n\n"
            ),
        )

    limiter = ProviderRateLimiter(
        max_concurrent_requests=1,
        requests_per_minute=1,
        acquire_timeout_ms=20,
    )
    request = {"capability": "general", "inference": _external_inference()}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = OpenAICompatibleModelProvider(
            provider="endpoint-a",
            model="server-side-model",
            base_url="https://provider.example/v1",
            api_key="test-only-secret",
            client=client,
            rate_limiter=limiter,
        )
        assert [chunk async for chunk in provider.stream_text("first", request)] == ["ok"]
        with pytest.raises(ModelProviderStreamError) as error:
            _ = [chunk async for chunk in provider.stream_text("second", request)]

    assert error.value.problem.code == "agent.provider_rate_limited"
    assert error.value.problem.status == 429
    assert calls == 1


@pytest.mark.asyncio
async def test_provider_rate_limiter_releases_its_slot_when_rate_wait_is_cancelled() -> None:
    """@brief 在等待速率窗口时取消不得泄漏并发位 / Cancellation while waiting for rate capacity must not leak a concurrency slot.

    @note 此回归测试将等待点替换为 event，避免用真实 sleep 构造时间脆弱测试；随后恢复
    正常实现并再次取得同一 limiter，证明外层 ``finally`` 已释放 semaphore。
    """
    limiter = ProviderRateLimiter(
        max_concurrent_requests=1,
        requests_per_minute=1,
        acquire_timeout_ms=1_000,
    )
    entered_rate_wait = asyncio.Event()
    never_release = asyncio.Event()
    async def blocked_wait_for_rate_slot() -> None:
        """@brief 将测试任务停在 rate-wait 临界点 / Stop the test task at the rate-wait boundary."""
        entered_rate_wait.set()
        await never_release.wait()

    async def acquire_then_wait() -> None:
        """@brief 请求一个 limiter slot 并停在速率等待 / Request a limiter slot and stop at rate wait."""
        async with limiter.limit():
            raise AssertionError("cancelled task must not enter the provider request body")

    with patch.object(limiter, "_wait_for_rate_slot", blocked_wait_for_rate_slot):
        waiting_task = asyncio.create_task(acquire_then_wait())
        await asyncio.wait_for(entered_rate_wait.wait(), timeout=0.2)
        waiting_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting_task

    async with asyncio.timeout(0.2):
        async with limiter.limit():
            pass


@pytest.mark.asyncio
async def test_external_provider_fails_closed_before_network_without_opt_in_or_region_match() -> None:
    """@brief 未明确同意外部处理或地域不匹配时绝不发送请求 / No network request occurs without external consent or region match."""
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        """@brief 记录意外网络调用 / Record an unexpected network call.

        @param request MockTransport 接收到的请求 / Request received by MockTransport.
        @return 不应被使用的 SSE 响应。
        """
        nonlocal calls
        del request
        calls += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = OpenAICompatibleModelProvider(
            provider="openrouter",
            model="server-side-model",
            base_url="https://provider.example/v1",
            api_key="test-only-secret",
            data_region="global",
            client=client,
        )
        with pytest.raises(ModelProviderStreamError) as consent_error:
            _ = [
                chunk
                async for chunk in provider.stream_text(
                    "不应外发", {"capability": "general", "inference": {}}
                )
            ]
        assert consent_error.value.problem.code == "agent.external_model_processing_not_allowed"

        with pytest.raises(ModelProviderStreamError) as region_error:
            _ = [
                chunk
                async for chunk in provider.stream_text(
                    "不应外发", {"capability": "general", "inference": _external_inference(region="cn")}
                )
            ]
        assert region_error.value.problem.code == "agent.data_region_unavailable"
    assert calls == 0


@pytest.mark.asyncio
async def test_fallback_requires_explicit_inference_consent() -> None:
    """@brief provider fallback 只有显式同意才会接收 prompt / A fallback receives a prompt only with explicit consent."""

    def handler(request: httpx.Request) -> httpx.Response:
        """@brief 返回首 provider 的可恢复故障 / Return a retryable primary-provider failure.

        @param request MockTransport 接收到的请求 / Request received by MockTransport.
        @return HTTP 503 响应。
        """
        del request
        return httpx.Response(503, headers={"content-type": "text/event-stream"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        primary = OpenAICompatibleModelProvider(
            provider="primary",
            model="primary-model",
            base_url="https://provider.example/v1",
            api_key="test-only-secret",
            client=client,
        )
        provider = FallbackModelProvider((primary, MockModelProvider()))
        denied_request = {"capability": "general", "inference": _external_inference(allow_fallback=False)}
        with pytest.raises(ModelProviderStreamError) as denied_error:
            _ = [chunk async for chunk in provider.stream_text("测试", denied_request)]
        assert denied_error.value.problem.code == "agent.provider_unavailable"

        accepted_request = {"capability": "general", "inference": _external_inference()}
        chunks = [chunk async for chunk in provider.stream_text("测试", accepted_request)]
    assert "".join(chunks).startswith("已收到你的请求")
