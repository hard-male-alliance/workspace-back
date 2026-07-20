"""@brief 可信代理身份边界的无网络测试 / Network-free tests for the trusted-proxy identity boundary."""

from __future__ import annotations

import json
import time
from pathlib import Path

import json5
import pytest
from fastapi.testclient import TestClient

from backend.app import create_app
from backend.config import BackendSettings, SecuritySettings
from backend.infrastructure.identity import (
    HEADER_ACTOR_ID,
    HEADER_AUTH_TIMESTAMP,
    HEADER_IDENTITY_SIGNATURE,
    HEADER_IDENTITY_VERSION,
    HEADER_RESOURCE_OWNER_ID,
    HEADER_WORKSPACE_ID,
    IDENTITY_SIGNATURE_VERSION,
    DevelopmentMockIdentityResolver,
    IdentityVerificationError,
    TrustedProxyHMACIdentityResolver,
    build_identity_resolver,
    peer_is_trusted_proxy,
    sign_trusted_proxy_assertion,
)
from workspace_shared.jsonc import ConfigurationError
from workspace_shared.tenancy import ActorScope

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Repository root directory."""

_TEST_SECRET = "0123456789abcdef0123456789abcdef"
"""@brief 仅测试用的 32-byte HMAC 密钥 / Test-only 32-byte HMAC secret."""

_NOW = 1_750_000_000
"""@brief 可重复测试的 Unix 当前秒 / Deterministic Unix current seconds for tests."""


def _signed_headers(
    *,
    method: str = "POST",
    path: str = "/api/v1/resumes",
    query_string: str = "",
    actor_id: str = "usr_klee",
    workspace_id: str = "ws_alpha",
    resource_owner_id: str = "usr_klee",
    timestamp: int = _NOW,
) -> dict[str, str]:
    """@brief 构建一个完整的可信代理断言 / Build a complete trusted-proxy assertion.

    @param method 已签名 HTTP 方法 / Signed HTTP method.
    @param path 已签名 raw path / Signed raw path.
    @param query_string 已签名 raw query / Signed raw query.
    @param actor_id actor 声明 / Actor claim.
    @param workspace_id workspace 声明 / Workspace claim.
    @param resource_owner_id owner 声明 / Owner claim.
    @param timestamp 签发 Unix 秒 / Issued Unix seconds.
    @return 供 resolver 消费的固定 header 集 / Fixed header set consumed by the resolver.
    """
    signature = sign_trusted_proxy_assertion(
        _TEST_SECRET,
        method=method,
        path=path,
        query_string=query_string,
        actor_id=actor_id,
        workspace_id=workspace_id,
        resource_owner_id=resource_owner_id,
        timestamp=timestamp,
    )
    return {
        HEADER_IDENTITY_VERSION: IDENTITY_SIGNATURE_VERSION,
        HEADER_ACTOR_ID: actor_id,
        HEADER_WORKSPACE_ID: workspace_id,
        HEADER_RESOURCE_OWNER_ID: resource_owner_id,
        HEADER_AUTH_TIMESTAMP: str(timestamp),
        HEADER_IDENTITY_SIGNATURE: signature,
    }


def _write_settings(
    tmp_path: Path,
    *,
    environment: str,
    security: dict[str, object] | None,
) -> Path:
    """@brief 生成仅供解析测试的 JSONC 等价配置 / Generate a JSONC-equivalent config for parsing tests.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param environment 目标环境标签 / Target environment label.
    @param security 可选 ``security`` 配置节 / Optional ``security`` configuration section.
    @return 新配置文件路径 / New configuration-file path.
    """
    payload = json5.loads((PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    payload["environment"] = environment
    if security is None:
        payload.pop("security", None)
    else:
        payload["security"] = security
    path = tmp_path / "config.jsonc"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_development_can_use_legacy_mock_default_but_staging_cannot(tmp_path: Path) -> None:
    """@brief 缺失安全节只在 development/test 回退 mock / Missing security falls back to mock only in development/test."""
    development_settings = BackendSettings.from_file(
        _write_settings(tmp_path, environment="development", security=None)
    )
    assert development_settings.security.identity_mode == "development_mock"

    staging_path = _write_settings(tmp_path, environment="staging", security=None)
    with pytest.raises(ConfigurationError, match="security section is required"):
        BackendSettings.from_file(staging_path)


def test_production_rejects_development_mock_identity_mode(tmp_path: Path) -> None:
    """@brief production 配置必须选择可验证 HMAC / Production config must select verifiable HMAC."""
    path = _write_settings(
        tmp_path,
        environment="production",
        security={
            "identity_mode": "development_mock",
            "trusted_proxy_hmac_secret_env": "AIWS_TRUSTED_PROXY_HMAC_SECRET",
            "trusted_proxy_max_clock_skew_seconds": 60,
        },
    )
    with pytest.raises(ConfigurationError, match="trusted_proxy_hmac"):
        BackendSettings.from_file(path)


def test_production_requires_postgresql_even_with_trusted_identity(tmp_path: Path) -> None:
    """@brief production 不能以 memory repository 启动 / Production cannot start with the memory repository."""
    path = _write_settings(
        tmp_path,
        environment="production",
        security={
            "identity_mode": "trusted_proxy_hmac",
            "trusted_proxy_hmac_secret_env": "AIWS_TRUSTED_PROXY_HMAC_SECRET",
            "trusted_proxy_max_clock_skew_seconds": 60,
        },
    )
    with pytest.raises(ConfigurationError, match=r"database\.mode must be postgresql"):
        BackendSettings.from_file(path)


def test_configuration_rejects_unsafe_outbound_proxy_url(tmp_path: Path) -> None:
    """@brief 出站 proxy 必须是受限的 HTTP(S) URL / Outbound proxy must be a constrained HTTP(S) URL."""
    path = _write_settings(tmp_path, environment="development", security=None)
    payload = json5.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    network = payload.get("network")
    assert isinstance(network, dict)
    network["outbound_proxy_url"] = "file:///tmp/proxy"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ConfigurationError, match=r"outbound_proxy_url"):
        BackendSettings.from_file(path)


def test_hmac_identity_verifies_all_claims_and_raw_request_target() -> None:
    """@brief HMAC 必须同时绑定三类范围声明、方法、path 和 query / HMAC must bind all scope claims, method, path, and query."""
    resolver = TrustedProxyHMACIdentityResolver(_TEST_SECRET, 30, clock=lambda: float(_NOW))
    headers = _signed_headers(query_string="include=history%2Ffull")
    scope = resolver.resolve(
        method="post",
        path=b"/api/v1/resumes",
        query_string=b"include=history%2Ffull",
        headers=headers,
    )
    assert scope == ActorScope("usr_klee", "ws_alpha", "usr_klee")

    with pytest.raises(IdentityVerificationError, match=r"identity\.signature_invalid"):
        resolver.resolve(
            method="POST",
            path="/api/v1/resumes",
            query_string="include=history%2Fother",
            headers=headers,
        )


def test_hmac_identity_rejects_forged_or_expired_assertions() -> None:
    """@brief 伪造 scope 声明与过期 timestamp 都必须 fail closed / Forged scope claims and expired timestamps must fail closed."""
    resolver = TrustedProxyHMACIdentityResolver(_TEST_SECRET, 30, clock=lambda: float(_NOW))
    forged_headers = _signed_headers()
    forged_headers[HEADER_RESOURCE_OWNER_ID] = "usr_attacker"
    with pytest.raises(IdentityVerificationError, match=r"identity\.signature_invalid") as forged_error:
        resolver.resolve(method="POST", path="/api/v1/resumes", headers=forged_headers)
    assert _TEST_SECRET not in str(forged_error.value)
    assert forged_headers[HEADER_IDENTITY_SIGNATURE] not in str(forged_error.value)

    expired_headers = _signed_headers(timestamp=_NOW - 31)
    with pytest.raises(IdentityVerificationError, match=r"identity\.timestamp_out_of_window"):
        resolver.resolve(method="POST", path="/api/v1/resumes", headers=expired_headers)


def test_trusted_proxy_source_accepts_only_configured_ip_networks(tmp_path: Path) -> None:
    """@brief 可信 HMAC 还必须来自已配置对端 CIDR / Trusted HMAC must also originate from a configured peer CIDR."""
    settings = BackendSettings.from_file(_write_settings(tmp_path, environment="development", security=None))
    networks = settings.network.trusted_proxy_cidrs
    assert peer_is_trusted_proxy("127.0.0.1", networks)
    assert not peer_is_trusted_proxy("198.51.100.10", networks)
    assert not peer_is_trusted_proxy("identity-proxy.internal", networks)


def test_mock_resolver_cannot_be_constructed_for_production() -> None:
    """@brief 直接构造 mock resolver 也不能绕过环境保护 / Direct construction cannot bypass the environment guard."""
    default_scope = ActorScope("usr_local", "ws_local", "usr_local")
    with pytest.raises(ConfigurationError, match="only allowed"):
        DevelopmentMockIdentityResolver(default_scope, environment="production")

    resolver = DevelopmentMockIdentityResolver(default_scope, environment="test")
    assert resolver.resolve(
        method="GET",
        path="/api/v1/resumes",
        headers={"X-Mock-Actor-Id": "usr_test"},
    ) == ActorScope("usr_test", "ws_local", "usr_local")


def test_identity_factory_requires_secret_without_exposing_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """@brief HMAC factory 只从环境取密钥，且缺失时不泄漏变量值 / HMAC factory reads only env secrets and leaks no value when absent."""
    security = SecuritySettings(
        identity_mode="trusted_proxy_hmac",
        trusted_proxy_hmac_secret_env="AIWS_TEST_IDENTITY_SECRET",
        trusted_proxy_max_clock_skew_seconds=60,
    )
    default_scope = ActorScope("usr_local", "ws_local", "usr_local")
    monkeypatch.delenv("AIWS_TEST_IDENTITY_SECRET", raising=False)
    with pytest.raises(ConfigurationError, match="secret environment variable is not set") as absent_error:
        build_identity_resolver(
            environment="production",
            default_scope=default_scope,
            security=security,
        )
    assert "AIWS_TEST_IDENTITY_SECRET" not in str(absent_error.value)

    monkeypatch.setenv("AIWS_TEST_IDENTITY_SECRET", _TEST_SECRET)
    resolver = build_identity_resolver(
        environment="production",
        default_scope=default_scope,
        security=security,
    )
    assert isinstance(resolver, TrustedProxyHMACIdentityResolver)


def test_http_middleware_rejects_unsigned_scope_headers_and_accepts_signed_scope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief HTTP 中间件只能接受经可信代理签名的生产身份 / HTTP middleware accepts production identity only when proxy-signed."""
    monkeypatch.setenv("AIWS_TRUSTED_PROXY_HMAC_SECRET", _TEST_SECRET)
    settings = BackendSettings.from_file(
        _write_settings(
            tmp_path,
            environment="development",
            security={
                "identity_mode": "trusted_proxy_hmac",
                "trusted_proxy_hmac_secret_env": "AIWS_TRUSTED_PROXY_HMAC_SECRET",
                "trusted_proxy_max_clock_skew_seconds": 300,
            },
        )
    )
    with TestClient(create_app(settings), client=("127.0.0.1", 50_000)) as client:
        rejected = client.get(
            "/api/v1/resumes",
            headers={"X-AIWS-Actor-Id": "usr_attacker", "X-AIWS-Workspace-Id": "ws_other"},
        )
        assert rejected.status_code == 401
        assert rejected.headers["content-type"].startswith("application/problem+json")
        accepted = client.get(
            "/api/v1/resumes",
            headers=_signed_headers(
                method="GET",
                path="/api/v1/resumes",
                timestamp=int(time.time()),
            ),
        )
        assert accepted.status_code == 200
        assert accepted.json()["items"] == []

    with TestClient(create_app(settings), client=("198.51.100.10", 50_001)) as untrusted_client:
        rejected_peer = untrusted_client.get(
            "/api/v1/resumes",
            headers=_signed_headers(
                method="GET",
                path="/api/v1/resumes",
                timestamp=int(time.time()),
            ),
        )
        assert rejected_peer.status_code == 401
