"""@brief 身份邮件 outbox 配置 fail-closed 测试 / Identity-email outbox configuration fail-closed tests."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from backend.config import BackendSettings
from workspace_shared.jsonc import ConfigurationError, load_jsonc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根目录 / Repository root."""


def _encoded_key(fill: int) -> str:
    """@brief 构造无 padding 的 256-bit base64url 测试 key / Build an unpadded 256-bit base64url test key."""

    return base64.urlsafe_b64encode(bytes([fill]) * 32).rstrip(b"=").decode("ascii")


def _write(root: dict[str, object], path: Path) -> Path:
    """@brief 写入隔离测试配置 / Write an isolated test configuration.

    @return 输入路径 / Input path.
    """

    path.write_text(json.dumps(root), encoding="utf-8")
    return path


def test_deployed_email_outbox_requires_aes_and_independent_hmac_keys(tmp_path: Path) -> None:
    """@brief staging/production 无 key 时启动失败 / Staging/production startup fails without outbox keys."""

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    root["environment"] = "production"
    root["database"]["mode"] = "postgresql"
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
    root["hosted_identity"]["email"].update(
        {
            "mode": "smtp",
            "from_address": "identity@example.test",
            "smtp_host": "smtp.example.test",
        }
    )
    root["hosted_identity"]["password_breach"]["mode"] = "pwned_passwords"

    with pytest.raises(ConfigurationError, match="outbox encryption and rate-limit keys"):
        BackendSettings.from_file(_write(root, tmp_path / "production.json"))


def test_outbox_decodes_exact_256_bit_keys_and_rejects_key_reuse(tmp_path: Path) -> None:
    """@brief keyring 只接受 256 bits 且禁止加密/HMAC key 复用 / Keyring requires 256 bits and domain separation."""

    root = load_jsonc(PROJECT_ROOT / "example.jsonc")
    outbox = root["hosted_identity"]["email"]["outbox"]
    outbox.update(
        {
            "active_key_id": "email-key-2026-07",
            "encryption_keys": {"email-key-2026-07": _encoded_key(7)},
            "rate_limit_hmac_key": _encoded_key(9),
        }
    )

    settings = BackendSettings.from_file(_write(root, tmp_path / "development.json"))

    assert settings.hosted_identity.email.outbox.active_key_id == "email-key-2026-07"
    assert settings.hosted_identity.email.outbox.encryption_keys[0].key == bytes([7]) * 32
    assert settings.hosted_identity.email.outbox.rate_limit_hmac_key == bytes([9]) * 32

    outbox["rate_limit_hmac_key"] = _encoded_key(7)
    with pytest.raises(ConfigurationError, match="must be distinct"):
        BackendSettings.from_file(_write(root, tmp_path / "reused-key.json"))

    outbox["rate_limit_hmac_key"] = _encoded_key(9)
    short_key = base64.urlsafe_b64encode(bytes([7]) * 31).rstrip(b"=").decode("ascii")
    outbox["encryption_keys"] = {"email-key-2026-07": short_key}
    with pytest.raises(ConfigurationError, match="exactly 256 bits"):
        BackendSettings.from_file(_write(root, tmp_path / "short-key.json"))
