"""@brief Knowledge V2 外部端口配置测试 / Knowledge V2 external-port configuration tests."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest

from backend.config import BackendSettings, KnowledgeS3UploadStorageSettings
from workspace_shared.jsonc import ConfigurationError, load_jsonc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根目录 / Repository root."""


def _encoded_key(fill: int) -> str:
    """@brief 构造无 padding 的 256-bit base64url key / Build an unpadded 256-bit base64url key.

    @param fill 测试 byte / Test byte.
    @return 编码 key / Encoded key.
    """

    return base64.urlsafe_b64encode(bytes([fill]) * 32).rstrip(b"=").decode("ascii")


def _write(root: dict[str, Any], path: Path) -> Path:
    """@brief 写入隔离配置 / Write an isolated configuration.

    @param root 配置对象 / Configuration object.
    @param path 输出路径 / Output path.
    @return 输入路径 / Input path.
    """

    path.write_text(json.dumps(root), encoding="utf-8")
    return path


def _postgres_root() -> dict[str, Any]:
    """@brief 构造 development PostgreSQL Knowledge 配置 / Build a development PostgreSQL Knowledge configuration.

    @return 带全部 durable Knowledge keys 的根对象 / Root mapping with all durable Knowledge keys.
    """

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    root["database"].update(
        {
            "mode": "postgresql",
            "application_dsn": "postgresql+asyncpg://app:password@db.example.test/workspace",
        }
    )
    connections = root["knowledge"]["connections"]
    connections.update(
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
    root["knowledge"]["uploads"]["storage"]["local"]["signing_hmac_key"] = (
        _encoded_key(14)
    )
    return root


def _production_root() -> dict[str, Any]:
    """@brief 构造可启动的 production Knowledge 配置 / Build a bootable production Knowledge configuration.

    @return S3/reject/durable-secret 根对象 / S3, rejecting-scanner, durable-secret root mapping.
    """

    root = _postgres_root()
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
            "from_address": "identity@example.test",
            "smtp_host": "smtp.example.test",
        }
    )
    root["hosted_identity"]["email"]["outbox"].update(
        {
            "active_key_id": "email-2026-07",
            "encryption_keys": {"email-2026-07": _encoded_key(1)},
            "rate_limit_hmac_key": _encoded_key(2),
        }
    )
    storage = root["knowledge"]["uploads"]["storage"]
    storage.update(
        {
            "mode": "s3",
            "local": None,
            "s3": {
                "endpoint": "https://objects.example.test",
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
    )
    root["knowledge"]["uploads"]["malware"] = {"mode": "reject", "clamav": None}
    root["interview"]["realtime"].update(
        {
            "signing_keyring": {
                "active_key_id": "interview-2026-07",
                "keys": {"interview-2026-07": _encoded_key(20)},
            },
            "signaling_url": "wss://realtime.hmalliances.org/v2/interview",
        }
    )
    return root


def test_secret_free_example_exposes_typed_secure_development_defaults() -> None:
    """@brief 公开示例可加载且没有伪造 durable secret / Public example loads without manufactured durable secrets.

    @return 无返回值 / No return value.
    """

    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")

    assert settings.knowledge.connections.providers == ()
    assert settings.knowledge.connections.provider_session_keyring.active_key_id is None
    assert settings.knowledge.connections.credential_fingerprint_hmac_key is None
    assert settings.knowledge.uploads.storage.mode == "local"
    assert settings.knowledge.uploads.malware.mode == "dev"
    assert settings.knowledge.source_network.allowed_schemes == ("https",)


def test_production_uses_s3_reject_scanner_and_independent_durable_keys(
    tmp_path: Path,
) -> None:
    """@brief production 接受 S3 与 fail-closed scanner / Production accepts S3 and a fail-closed scanner.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    settings = BackendSettings.from_file(
        _write(_production_root(), tmp_path / "production.json")
    )

    assert isinstance(settings.knowledge.uploads.storage, KnowledgeS3UploadStorageSettings)
    assert settings.knowledge.connections.provider_session_keyring.keys[0].key == bytes([10]) * 32
    assert settings.knowledge.connections.credential_keyring.keys[0].key == bytes([11]) * 32
    assert settings.knowledge.connections.credential_fingerprint_hmac_key == bytes([12]) * 32
    assert settings.knowledge.connections.credential_reference_hmac_key == bytes([13]) * 32


def test_production_rejects_local_storage_dev_scanner_and_plain_http(tmp_path: Path) -> None:
    """@brief production 禁止本地存储、allow-all scanner 与 HTTP / Production forbids local storage, allow-all scanning, and HTTP.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    root = _production_root()
    local = load_jsonc(PROJECT_ROOT / "example.jsonc")["knowledge"]["uploads"]["storage"][
        "local"
    ]
    local["signing_hmac_key"] = _encoded_key(14)
    root["knowledge"]["uploads"]["storage"] = {
        "mode": "local",
        "local": local,
        "s3": None,
    }
    with pytest.raises(ConfigurationError, match="must be s3"):
        BackendSettings.from_file(_write(root, tmp_path / "local.json"))

    root = _production_root()
    root["knowledge"]["uploads"]["malware"] = {"mode": "dev", "clamav": None}
    with pytest.raises(ConfigurationError, match="cannot be dev"):
        BackendSettings.from_file(_write(root, tmp_path / "dev-scanner.json"))

    root = _production_root()
    root["knowledge"]["source_network"]["allowed_schemes"] = ["http", "https"]
    with pytest.raises(ConfigurationError, match="cannot include http"):
        BackendSettings.from_file(_write(root, tmp_path / "plain-http.json"))


def test_memory_mode_rejects_configured_connection_provider(tmp_path: Path) -> None:
    """@brief memory 无 durable vault 时不能声称 provider 可用 / Memory mode cannot advertise a provider without a durable vault.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    root["knowledge"]["connections"]["providers"] = [
        {
            "provider": "github",
            "client_id": "client-id",
            "authorization_endpoint": "https://github.example.test/authorize",
            "token_endpoint": "https://github.example.test/token",
            "device_authorization_endpoint": None,
            "redirect_uri": "https://workspace.example.test/oauth/callback",
            "allowed_scopes": ["repo:read"],
            "api_token_validation": None,
            "revocation_endpoint": None,
        }
    ]

    with pytest.raises(ConfigurationError, match="must be empty in memory"):
        BackendSettings.from_file(_write(root, tmp_path / "memory-provider.json"))


def test_postgresql_provider_parses_exact_endpoints_scopes_and_api_validation(
    tmp_path: Path,
) -> None:
    """@brief PostgreSQL provider registry 保留闭合 capability / PostgreSQL provider registry preserves closed capabilities.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    root = _postgres_root()
    root["knowledge"]["connections"]["providers"] = [
        {
            "provider": "github",
            "client_id": "client-id",
            "authorization_endpoint": "https://github.example.test/authorize",
            "token_endpoint": "https://github.example.test/token",
            "device_authorization_endpoint": "https://github.example.test/device",
            "redirect_uri": "https://workspace.example.test/oauth/callback",
            "allowed_scopes": ["repo:read", "user:read"],
            "api_token_validation": {
                "endpoint": "https://api.github.example.test/me",
                "method": "GET",
                "authorization_scheme": "Bearer",
                "scopes_field": "scopes",
            },
            "revocation_endpoint": "https://github.example.test/revoke",
        }
    ]

    provider = BackendSettings.from_file(_write(root, tmp_path / "provider.json")).knowledge.connections.providers[0]

    assert provider.provider == "github"
    assert provider.allowed_scopes == ("repo:read", "user:read")
    assert provider.api_token_validation is not None
    assert provider.api_token_validation.method == "GET"


def test_knowledge_rejects_unknown_fields_duplicate_json_keys_and_key_reuse(
    tmp_path: Path,
) -> None:
    """@brief 配置 typo、重复 key 与跨用途密钥复用均 fail closed / Typos, duplicate keys, and cross-purpose key reuse fail closed.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    """

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    root["knowledge"]["search"]["candidate_multipler"] = 4
    with pytest.raises(ConfigurationError, match="unknown keys"):
        BackendSettings.from_file(_write(root, tmp_path / "unknown.json"))

    duplicate = (PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8").replace(
        '"maximum_attempts": 12,',
        '"maximum_attempts": 12, "maximum_attempts": 13,',
    )
    duplicate_path = tmp_path / "duplicate.jsonc"
    duplicate_path.write_text(duplicate, encoding="utf-8")
    with pytest.raises(ConfigurationError, match="duplicate object key"):
        BackendSettings.from_file(duplicate_path)

    root = _postgres_root()
    root["knowledge"]["connections"]["credential_reference_hmac_key"] = _encoded_key(12)
    with pytest.raises(ConfigurationError, match="must be distinct across purposes"):
        BackendSettings.from_file(_write(root, tmp_path / "reused.json"))


@pytest.mark.parametrize(
    ("path", "value", "message"),
    (
        (("uploads", "maximum_archive_depth"), 11, "maximum_archive_depth"),
        (("search", "candidate_multiplier"), 21, "candidate_multiplier"),
        (("index", "embedding_batch_size"), 513, "embedding_batch_size"),
        (("source_network", "maximum_redirects"), 21, "maximum_redirects"),
    ),
)
def test_knowledge_resource_budgets_have_code_level_caps(
    tmp_path: Path,
    path: tuple[str, str],
    value: int,
    message: str,
) -> None:
    """@brief 配置不能抬高 worker/search/index 硬上限 / Config cannot raise worker/search/index hard caps.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @param path Knowledge 子节与字段 / Knowledge subsection and field.
    @param value 越界值 / Out-of-bound value.
    @param message 预期安全路径 / Expected safe path.
    """

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    section, field = path
    root["knowledge"][section][field] = value

    with pytest.raises(ConfigurationError, match=message):
        BackendSettings.from_file(_write(root, tmp_path / f"{section}.json"))
