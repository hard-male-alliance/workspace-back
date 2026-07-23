"""@brief 身份邮件 AEAD、频控与 retry 单元测试 / Identity-email AEAD, rate-limit, and retry unit tests."""

from __future__ import annotations

from datetime import timedelta

import pytest

from backend.domain.ports import IdentityEmailRateLimitExceeded
from backend.infrastructure.identity_email import MemoryIdentityEmailSender
from backend.infrastructure.identity_email_outbox import (
    ClaimedIdentityEmail,
    IdentityEmailKeyring,
    IdentityEmailPayloadError,
    identity_email_retry_delay,
)

KEY = bytes(range(32))
"""@brief 固定测试 AES-256 key / Fixed test AES-256 key."""


def test_aes_gcm_round_trip_binds_row_kind_key_and_aad_version() -> None:
    """@brief 合法 row 可解密，metadata 改写 fail closed / Valid row decrypts; metadata rewriting fails closed."""

    keyring = IdentityEmailKeyring("email-key-2026-07", {"email-key-2026-07": KEY})
    encrypted = keyring.encrypt(
        "emailmsg_00000001",
        "verification_code",
        recipient="klee@example.test",
        code="314159",
    )
    claimed = ClaimedIdentityEmail(
        "emailmsg_00000001",
        "verification_code",
        encrypted.key_id,
        encrypted.aad_version,
        encrypted.nonce,
        encrypted.ciphertext,
        1,
    )

    message = keyring.decrypt(claimed)

    assert message.recipient == "klee@example.test"
    assert message.code == "314159"
    assert b"klee@example.test" not in encrypted.ciphertext
    assert b"314159" not in encrypted.ciphertext
    with pytest.raises(IdentityEmailPayloadError):
        keyring.decrypt(
            ClaimedIdentityEmail(
                "emailmsg_other0001",
                claimed.message_kind,
                claimed.key_id,
                claimed.aad_version,
                claimed.nonce,
                claimed.ciphertext,
                claimed.attempts,
            )
        )
    with pytest.raises(IdentityEmailPayloadError):
        keyring.decrypt(
            ClaimedIdentityEmail(
                claimed.id,
                claimed.message_kind,
                claimed.key_id,
                claimed.aad_version,
                claimed.nonce,
                claimed.ciphertext[:-1] + bytes([claimed.ciphertext[-1] ^ 1]),
                claimed.attempts,
            )
        )


def test_key_rotation_decrypts_old_rows_but_encrypts_only_with_active_key() -> None:
    """@brief rotation overlap 保留旧解密 key / Rotation overlap retains old decryption key."""

    old = IdentityEmailKeyring("old", {"old": KEY})
    encrypted = old.encrypt(
        "emailmsg_00000002",
        "recovery_notification",
        recipient="klee@example.test",
        code=None,
    )
    claimed = ClaimedIdentityEmail(
        "emailmsg_00000002",
        "recovery_notification",
        encrypted.key_id,
        encrypted.aad_version,
        encrypted.nonce,
        encrypted.ciphertext,
        1,
    )
    rotated = IdentityEmailKeyring("new", {"old": KEY, "new": bytes(reversed(KEY))})

    assert rotated.decrypt(claimed).recipient == "klee@example.test"
    assert (
        rotated.encrypt(
            "emailmsg_00000003",
            "recovery_notification",
            recipient="klee@example.test",
            code=None,
        ).key_id
        == "new"
    )
    with pytest.raises(IdentityEmailPayloadError):
        IdentityEmailKeyring("new", {"new": bytes(reversed(KEY))}).decrypt(claimed)


def test_equal_jitter_retry_is_exponential_and_hard_capped() -> None:
    """@brief retry delay 指数增长且 jitter 不突破 cap / Retry delay grows exponentially without crossing the cap."""

    def upper(_lower: float, ceiling: float) -> float:
        """@brief 返回 jitter 区间上界 / Return the upper jitter bound."""

        return ceiling

    assert identity_email_retry_delay(
        1,
        base=timedelta(seconds=5),
        cap=timedelta(seconds=60),
        jitter=upper,
    ) == timedelta(seconds=5)
    assert identity_email_retry_delay(
        4,
        base=timedelta(seconds=5),
        cap=timedelta(seconds=60),
        jitter=upper,
    ) == timedelta(seconds=40)
    assert identity_email_retry_delay(
        100,
        base=timedelta(seconds=5),
        cap=timedelta(seconds=60),
        jitter=upper,
    ) == timedelta(seconds=60)


async def test_memory_mode_preserves_three_axis_rate_limit_contract() -> None:
    """@brief development adapter 仍保持公开 429 语义 / Development adapter preserves public rate-limit semantics."""

    sender = MemoryIdentityEmailSender()
    async with sender.atomic():
        await sender.send_verification_code(
            "klee@example.test",
            "123456",
            browser_session_id="idsess_device0001",
            network_identifier="203.0.113.10",
            limit_per_hour=1,
        )
    with pytest.raises(IdentityEmailRateLimitExceeded):
        async with sender.atomic():
            await sender.send_verification_code(
                "klee@example.test",
                "654321",
                browser_session_id="idsess_device0001",
                network_identifier="203.0.113.10",
                limit_per_hour=1,
            )

    assert sender.code_for("klee@example.test") == "123456"
