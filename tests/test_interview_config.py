"""@brief Interview V2 realtime 配置的 fail-closed 测试 / Fail-closed Interview V2 realtime configuration tests."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest

from backend.config import BackendSettings
from workspace_shared.jsonc import ConfigurationError, load_jsonc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根目录 / Repository root."""


def _encoded_key(fill: int) -> str:
    """@brief 构造无 padding 的 256-bit base64url key / Build an unpadded 256-bit base64url key.

    @param fill 测试 byte / Test byte.
    @return 规范编码 key / Canonically encoded key.
    """

    return base64.urlsafe_b64encode(bytes([fill]) * 32).rstrip(b"=").decode("ascii")


def _write(root: dict[str, Any], path: Path) -> Path:
    """@brief 写入隔离配置 / Write an isolated configuration.

    @param root 配置根对象 / Root mapping.
    @param path 输出路径 / Output path.
    @return 输入路径 / Input path.
    """

    path.write_text(json.dumps(root), encoding="utf-8")
    return path


def _configured_development_root() -> dict[str, Any]:
    """@brief 构造显式启用 realtime 的 development 配置 / Build a development config with realtime enabled.

    @return 配置根对象 / Root mapping.
    """

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    root["interview"]["realtime"].update(
        {
            "signing_keyring": {
                "active_key_id": "interview-2026-07",
                "keys": {"interview-2026-07": _encoded_key(20)},
            },
            "signaling_url": "wss://realtime.hmalliances.org/v2/interview",
            "ice_urls": [
                "stun:stun.hmalliances.org:3478",
                "turns:turn.hmalliances.org:5349?transport=tcp",
            ],
            "turn_shared_secret": _encoded_key(21),
        }
    )
    return root


def _production_root() -> dict[str, Any]:
    """@brief 构造所有部署门禁都完整的 production 配置 / Build a production config satisfying every deployment gate.

    @return 配置根对象 / Root mapping.
    """

    root = _configured_development_root()
    root["environment"] = "production"
    root["network"]["public_base_url"] = "https://api.hmalliances.org:8022"
    root["resume_rendering"]["adapter"] = "xelatex"
    root["ai"].update(
        {
            "provider": "openai-compatible",
            "model": "production-chat-model",
            "api_key": "test-only-model-key",
            "base_url": "https://models.example.test/v1",
            "embedding_provider": "openai-compatible",
            "embedding_model": "production-embedding-model",
            "embedding_model_revision": "2026-07",
        }
    )
    root["database"].update(
        {
            "mode": "postgresql",
            "application_dsn": (
                "postgresql+asyncpg://app:password@db.hmalliances.org/workspace"
            ),
        }
    )
    root["security"].update(
        {
            "identity_mode": "disabled",
            "trusted_proxy_hmac_secret": None,
            "cursor_hmac_secret": "cursor-signing-secret-that-has-32-bytes",
            "sensitive_idempotency_hmac_secret": (
                "sensitive-idempotency-secret-that-has-32-bytes"
            ),
        }
    )
    root["hosted_identity"]["password_breach"]["mode"] = "pwned_passwords"
    root["hosted_identity"]["email"].update(
        {
            "mode": "smtp",
            "from_address": "identity@hmalliances.org",
            "smtp_host": "smtp.hmalliances.org",
        }
    )
    root["hosted_identity"]["email"]["outbox"].update(
        {
            "active_key_id": "email-2026-07",
            "encryption_keys": {"email-2026-07": _encoded_key(1)},
            "rate_limit_hmac_key": _encoded_key(2),
        }
    )
    root["knowledge"]["connections"].update(
        {
            "provider_session_keyring": {
                "active_key_id": "launch-2026-07",
                "keys": {"launch-2026-07": _encoded_key(10)},
            },
            "credential_keyring": {
                "active_key_id": "vault-2026-07",
                "keys": {"vault-2026-07": _encoded_key(11)},
            },
            "credential_fingerprint_hmac_key": _encoded_key(12),
            "credential_reference_hmac_key": _encoded_key(13),
        }
    )
    root["knowledge"]["uploads"]["storage"] = {
        "mode": "s3",
        "local": None,
        "s3": {
            "endpoint": "https://objects.hmalliances.org",
            "region": "private-1",
            "bucket": "knowledge",
            "access_key_id": "test-access-key",
            "secret_access_key": "test-secret-access-key",
            "session_token": None,
            "object_prefix": "aiws-uploads",
            "connect_timeout_ms": 3000,
            "read_timeout_ms": 30000,
        },
    }
    root["knowledge"]["uploads"]["malware"] = {"mode": "reject", "clamav": None}
    return root


def test_public_example_disables_realtime_without_manufactured_secrets() -> None:
    """@brief 公开示例保持可加载但不伪造 realtime secret / Public example loads without manufactured realtime secrets."""

    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")

    assert settings.interview.realtime.signing_keyring.active_key_id is None
    assert settings.interview.realtime.active_signing_key is None
    assert settings.interview.realtime.signaling_url is None
    assert settings.interview.realtime.allowed_transports == ("webrtc", "websocket")
    assert settings.interview.realtime.credential_ttl_seconds == 300
    assert settings.interview.realtime.ice_urls == ()


def test_configured_realtime_decodes_key_and_preserves_transport_policy(
    tmp_path: Path,
) -> None:
    """@brief 显式配置保留强类型策略 / Explicit configuration preserves the typed policy.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    settings = BackendSettings.from_file(
        _write(_configured_development_root(), tmp_path / "development.json")
    )

    assert settings.interview.realtime.active_signing_key == bytes([20]) * 32
    assert settings.interview.realtime.signaling_url == (
        "wss://realtime.hmalliances.org/v2/interview"
    )
    assert settings.interview.realtime.ice_urls[1].startswith("turns:")
    assert settings.interview.realtime.turn_shared_secret == _encoded_key(21)


def test_deployed_realtime_requires_keyring_and_non_placeholder_endpoint(
    tmp_path: Path,
) -> None:
    """@brief 部署环境拒绝缺失与占位 endpoint / Deployed environments reject missing and placeholder endpoints.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    settings = BackendSettings.from_file(
        _write(_production_root(), tmp_path / "production.json")
    )
    assert settings.interview.realtime.active_signing_key == bytes([20]) * 32

    root = _production_root()
    root["interview"]["realtime"]["signing_keyring"] = {
        "active_key_id": None,
        "keys": {},
    }
    root["interview"]["realtime"]["signaling_url"] = None
    with pytest.raises(ConfigurationError, match="required outside development/test"):
        BackendSettings.from_file(_write(root, tmp_path / "missing.json"))

    root = _production_root()
    root["interview"]["realtime"]["signaling_url"] = (
        "wss://realtime.example.test/v2/interview"
    )
    with pytest.raises(ConfigurationError, match="placeholder host"):
        BackendSettings.from_file(_write(root, tmp_path / "placeholder.json"))


def test_realtime_bounds_and_ice_uri_grammar_are_closed(tmp_path: Path) -> None:
    """@brief TTL、heartbeat、transport 与 ICE URI 不可越过端口硬边界 / Realtime policy cannot exceed adapter hard bounds.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    root = _configured_development_root()
    root["interview"]["realtime"]["credential_ttl_seconds"] = 901
    with pytest.raises(ConfigurationError, match="must not exceed 900"):
        BackendSettings.from_file(_write(root, tmp_path / "ttl.json"))

    root = _configured_development_root()
    root["interview"]["realtime"]["heartbeat_interval_ms"] = 999
    with pytest.raises(ConfigurationError, match="between 1000 and 120000"):
        BackendSettings.from_file(_write(root, tmp_path / "heartbeat.json"))

    root = _configured_development_root()
    root["interview"]["realtime"]["allowed_transports"] = ["webtransport"]
    with pytest.raises(ConfigurationError, match="webrtc and websocket"):
        BackendSettings.from_file(_write(root, tmp_path / "transport.json"))

    root = _configured_development_root()
    root["interview"]["realtime"]["ice_urls"] = [
        "turn:turn.hmalliances.org:3478?credential=leaked"
    ]
    with pytest.raises(ConfigurationError, match="invalid STUN/TURN URI"):
        BackendSettings.from_file(_write(root, tmp_path / "ice.json"))


def test_interview_key_domain_is_distinct_from_knowledge_and_idempotency(
    tmp_path: Path,
) -> None:
    """@brief Interview key 不得与 Knowledge/idempotency 复用 / Interview keys cannot be reused by Knowledge or idempotency.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    root = _production_root()
    root["interview"]["realtime"]["signing_keyring"]["keys"] = {
        "interview-2026-07": _encoded_key(10)
    }
    with pytest.raises(ConfigurationError, match="Interview realtime and Knowledge"):
        BackendSettings.from_file(_write(root, tmp_path / "knowledge-reuse.json"))

    root = _configured_development_root()
    root["security"]["sensitive_idempotency_hmac_secret"] = _encoded_key(20)
    with pytest.raises(ConfigurationError, match="sensitive-idempotency"):
        BackendSettings.from_file(_write(root, tmp_path / "idempotency-reuse.json"))
