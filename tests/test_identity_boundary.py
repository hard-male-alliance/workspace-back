"""@brief 可信代理身份边界的无网络测试 / Network-free tests for the trusted-proxy identity boundary."""

from __future__ import annotations

import json
import time
from dataclasses import replace
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
    DisabledLegacyIdentityResolver,
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
    if environment not in {"development", "test"}:
        payload["hosted_identity"]["email"].update(
            {
                "mode": "smtp",
                "from_address": "identity@example.test",
                "smtp_host": "smtp.example.test",
            }
        )
        payload["hosted_identity"]["password_breach"]["mode"] = "pwned_passwords"
    if security is None:
        payload.pop("security", None)
    else:
        security.setdefault(
            "sensitive_idempotency_hmac_secret",
            (
                "test-sensitive-idempotency-secret-at-least-32-bytes"
                if environment not in {"development", "test"}
                else None
            ),
        )
        payload["security"] = security
    path = tmp_path / "config.jsonc"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_security_section_is_always_explicit(tmp_path: Path) -> None:
    """@brief 所有环境都必须从唯一配置显式读取身份设置 / Every environment requires explicit identity settings."""
    path = _write_settings(tmp_path, environment="development", security=None)
    with pytest.raises(ConfigurationError, match="security"):
        BackendSettings.from_file(path)


def test_production_rejects_development_mock_identity_mode(tmp_path: Path) -> None:
    """@brief production 不得保留开发 mock 身份边界 / Production must not retain the development mock identity boundary."""
    path = _write_settings(
        tmp_path,
        environment="production",
        security={
            "identity_mode": "development_mock",
            "trusted_proxy_hmac_secret": None,
            "cursor_hmac_secret": "test-cursor-secret-at-least-32-bytes",
            "trusted_proxy_max_clock_skew_seconds": 60,
        },
    )
    with pytest.raises(ConfigurationError, match="only allowed in development/test"):
        BackendSettings.from_file(path)


def test_production_requires_postgresql_with_legacy_identity_disabled(tmp_path: Path) -> None:
    """@brief production 不能以 memory repository 启动 / Production cannot start with the memory repository."""
    path = _write_settings(
        tmp_path,
        environment="production",
        security={
            "identity_mode": "disabled",
            "trusted_proxy_hmac_secret": None,
            "cursor_hmac_secret": "test-cursor-secret-at-least-32-bytes",
            "trusted_proxy_max_clock_skew_seconds": 60,
        },
    )
    with pytest.raises(ConfigurationError, match=r"database\.mode must be postgresql"):
        BackendSettings.from_file(path)


def test_cursor_hmac_secret_is_explicit_and_production_safe(tmp_path: Path) -> None:
    """@brief Cursor 密钥必须足够长且生产环境不得随机生成 / Cursor secrets are strong and explicit in production.

    @param tmp_path pytest 临时目录 / Pytest temporary directory.
    """
    short_path = _write_settings(
        tmp_path,
        environment="development",
        security={
            "identity_mode": "development_mock",
            "trusted_proxy_hmac_secret": None,
            "cursor_hmac_secret": "too-short",
            "trusted_proxy_max_clock_skew_seconds": 60,
        },
    )
    with pytest.raises(ConfigurationError, match="cursor_hmac_secret"):
        BackendSettings.from_file(short_path)

    production_path = _write_settings(
        tmp_path,
        environment="production",
        security={
            "identity_mode": "disabled",
            "trusted_proxy_hmac_secret": None,
            "cursor_hmac_secret": None,
            "trusted_proxy_max_clock_skew_seconds": 60,
        },
    )
    with pytest.raises(ConfigurationError, match="cursor_hmac_secret"):
        BackendSettings.from_file(production_path)


def test_configuration_rejects_unsafe_outbound_proxy_url(tmp_path: Path) -> None:
    """@brief 出站 proxy 必须是受限的 HTTP(S) URL / Outbound proxy must be a constrained HTTP(S) URL."""
    path = _write_settings(
        tmp_path,
        environment="development",
        security={
            "identity_mode": "development_mock",
            "trusted_proxy_hmac_secret": None,
            "cursor_hmac_secret": None,
            "trusted_proxy_max_clock_skew_seconds": 300,
        },
    )
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
    settings = BackendSettings.from_file(
        _write_settings(
            tmp_path,
            environment="development",
            security={
                "identity_mode": "development_mock",
                "trusted_proxy_hmac_secret": None,
                "cursor_hmac_secret": None,
                "trusted_proxy_max_clock_skew_seconds": 300,
            },
        )
    )
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


def test_legacy_identity_factory_reads_direct_secret_without_exposing_it() -> None:
    """@brief 显式 legacy HMAC factory 只读直接配置且 repr 不泄密 / Explicit legacy HMAC factory reads direct config without repr leakage."""
    security = SecuritySettings(
        identity_mode="trusted_proxy_hmac",
        trusted_proxy_hmac_secret=_TEST_SECRET,
        cursor_hmac_secret="test-cursor-secret-at-least-32-bytes",
        sensitive_idempotency_hmac_secret="test-sensitive-idempotency-secret-32-bytes",
        trusted_proxy_max_clock_skew_seconds=60,
    )
    default_scope = ActorScope("usr_local", "ws_local", "usr_local")
    resolver = build_identity_resolver(
        environment="development",
        default_scope=default_scope,
        security=security,
    )
    assert isinstance(resolver, TrustedProxyHMACIdentityResolver)
    assert _TEST_SECRET not in repr(security)
    assert "test-cursor-secret-at-least-32-bytes" not in repr(security)
    assert "test-sensitive-idempotency-secret-32-bytes" not in repr(security)


def test_disabled_legacy_identity_resolver_fails_closed() -> None:
    """@brief V2-only 运行时的 legacy identity 端口始终拒绝 / The V2-only runtime's legacy identity port always fails closed."""

    security = SecuritySettings(
        identity_mode="disabled",
        trusted_proxy_hmac_secret=None,
        cursor_hmac_secret="test-cursor-secret-at-least-32-bytes",
        sensitive_idempotency_hmac_secret="test-sensitive-idempotency-secret-32-bytes",
        trusted_proxy_max_clock_skew_seconds=60,
    )
    resolver = build_identity_resolver(
        environment="production",
        default_scope=ActorScope("usr_local", "ws_local", "usr_local"),
        security=security,
    )

    assert isinstance(resolver, DisabledLegacyIdentityResolver)
    with pytest.raises(IdentityVerificationError, match=r"identity\.legacy_disabled"):
        resolver.resolve(
            method="GET",
            path="/api/v1/resumes",
            headers=_signed_headers(method="GET"),
        )


def test_legacy_v1_http_surface_is_closed_by_default() -> None:
    """@brief 默认应用工厂不挂载 API V1 路由 / The default application factory does not mount API V1 routes."""

    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")

    with TestClient(create_app(settings)) as client:
        response = client.get("/api/v1/resumes")

    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}


def test_explicit_legacy_v1_migration_rejects_unsigned_and_untrusted_scope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """@brief 显式 V1 迁移边界只接受可信对端签名的身份 / The explicit V1 migration boundary accepts only trusted-peer-signed identity."""
    settings = BackendSettings.from_file(
        _write_settings(
            tmp_path,
            environment="development",
            security={
                "identity_mode": "trusted_proxy_hmac",
                "trusted_proxy_hmac_secret": _TEST_SECRET,
                "cursor_hmac_secret": "test-cursor-secret-at-least-32-bytes",
                "trusted_proxy_max_clock_skew_seconds": 300,
            },
        )
    )
    settings = replace(settings, api=replace(settings.api, legacy_v1_enabled=True))
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
