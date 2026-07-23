"""@brief 身份邮件的本地队列与 SMTP transport / Local identity-email queue and SMTP transport."""

from __future__ import annotations

import asyncio
import hashlib
import smtplib
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from email.message import EmailMessage
from typing import Protocol

from backend.config import IdentityEmailSettings
from backend.domain.ports import IdentityEmailRateLimitExceeded


class IdentityEmailTransport(Protocol):
    """@brief outbox worker 使用的无持久化 transport / Non-persistent transport used by the outbox worker."""

    async def send_verification_code(self, recipient: str, code: str) -> None:
        """@brief 发送一封验证码邮件 / Send one verification-code message."""

    async def send_recovery_notification(self, recipient: str) -> None:
        """@brief 发送一封密码恢复通知 / Send one account-recovery notice."""


class MemoryIdentityEmailSender:
    """@brief 仅供 development/test 的内存事务队列 / In-memory transactional queue for development/test only."""

    def __init__(self) -> None:
        """@brief 初始化空队列与有界频控状态 / Initialize empty delivery and bounded-budget state."""

        self._codes: dict[str, str] = {}
        self.recovery_notifications: list[str] = []
        self._budgets: dict[tuple[str, bytes], tuple[int, int]] = {}
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def atomic(self) -> AsyncIterator[None]:
        """@brief 串行化本地测试邮件状态 / Serialize local test email state.

        @return 异步临界区 / Asynchronous critical section.
        """

        async with self._lock:
            yield

    async def send_verification_code(
        self,
        recipient: str,
        code: str,
        *,
        browser_session_id: str = "",
        network_identifier: str = "",
        limit_per_hour: int = 5,
    ) -> None:
        """@brief 在内存模式消费额度并记录验证码 / Consume local budgets and record a code.

        @param recipient 规范化收件地址 / Normalized recipient address.
        @param code 单次验证码 / One-time verification code.
        @param browser_session_id 浏览器绑定 / Browser binding.
        @param network_identifier 可信网络标识 / Trusted network identifier.
        @param limit_per_hour 每维每小时上限 / Per-dimension hourly limit.
        @raise IdentityEmailRateLimitExceeded 任一维额度耗尽 / Any dimension is exhausted.
        """

        if not browser_session_id and not network_identifier:
            self._codes[recipient] = code
            return
        if limit_per_hour < 1:
            raise ValueError("identity email rate limit must be positive")
        window = int(datetime.now(UTC).timestamp()) // 3_600
        dimensions = (
            ("account", _budget_digest(recipient)),
            ("device", _budget_digest(browser_session_id)),
            ("network", _budget_digest(network_identifier)),
        )
        if any(
            self._budgets.get(dimension, (window, 0)) == (window, limit_per_hour)
            or (
                self._budgets.get(dimension, (window, 0))[0] == window
                and self._budgets.get(dimension, (window, 0))[1] >= limit_per_hour
            )
            for dimension in dimensions
        ):
            raise IdentityEmailRateLimitExceeded
        for dimension in dimensions:
            prior_window, count = self._budgets.get(dimension, (window, 0))
            self._budgets[dimension] = (window, count + 1 if prior_window == window else 1)
        self._codes[recipient] = code

    async def send_recovery_notification(self, recipient: str) -> None:
        """@brief 记录一封本地恢复通知 / Record one local recovery notification.

        @param recipient 收件地址 / Recipient address.
        """

        self.recovery_notifications.append(recipient)

    def code_for(self, recipient: str) -> str:
        """@brief 仅向进程内测试返回验证码 / Return a code only to in-process tests.

        @param recipient 收件地址 / Recipient address.
        @return 最近接纳的明文验证码 / Most recently accepted plaintext code.
        """

        return self._codes[recipient]


def _budget_digest(value: str) -> bytes:
    """@brief 摘要内存频控维度 / Digest an in-memory rate-limit dimension.

    @param value 不应作为 dict key 留存的原值 / Plain value that must not remain as a dict key.
    @return 32-byte SHA-256 摘要 / 32-byte SHA-256 digest.
    """

    return hashlib.sha256(value.encode("utf-8")).digest()


class SmtpIdentityEmailSender:
    """@brief 使用 STARTTLS 的 SMTP transport / SMTP transport using STARTTLS."""

    def __init__(self, settings: IdentityEmailSettings) -> None:
        """@brief 绑定已校验的 SMTP 设置 / Bind validated SMTP settings.

        @param settings SMTP host、sender 与可选凭据 / SMTP host, sender, and optional credentials.
        @raise ValueError 必要字段缺失 / Required fields are missing.
        """

        if settings.smtp_host is None or settings.from_address is None:
            raise ValueError("SMTP identity email settings are incomplete")
        self._settings = settings

    async def send_verification_code(self, recipient: str, code: str) -> None:
        """@brief 在线程中同步提交验证码邮件 / Submit a verification message in a thread.

        @param recipient 收件地址 / Recipient address.
        @param code 单次验证码 / One-time code.
        """

        message = self._message(
            recipient,
            "Your AI Job Workspace verification code",
            f"Your verification code is {code}. It expires in 10 minutes and can be used once.",
        )
        await asyncio.to_thread(self._send, message)

    async def send_recovery_notification(self, recipient: str) -> None:
        """@brief 在线程中同步提交恢复通知 / Submit a recovery notice in a thread.

        @param recipient 收件地址 / Recipient address.
        """

        message = self._message(
            recipient,
            "Your AI Job Workspace account was recovered",
            "Your password was changed and existing sessions were revoked. "
            "If this was not you, contact support immediately.",
        )
        await asyncio.to_thread(self._send, message)

    def _message(self, recipient: str, subject: str, body: str) -> EmailMessage:
        """@brief 构造无附件文本邮件 / Build a plain-text message without attachments.

        @return 可交给 SMTP 的 EmailMessage / Message ready for SMTP submission.
        """

        message = EmailMessage()
        message["From"] = self._settings.from_address
        message["To"] = recipient
        message["Subject"] = subject
        message.set_content(body)
        return message

    def _send(self, message: EmailMessage) -> None:
        """@brief 在 worker thread 完成 SMTP 协议 / Complete SMTP submission in a worker thread.

        @param message 已构造邮件 / Constructed message.
        """

        assert self._settings.smtp_host is not None
        context = ssl.create_default_context()
        with smtplib.SMTP(
            self._settings.smtp_host,
            self._settings.smtp_port,
            timeout=15,
        ) as client:
            client.ehlo()
            if self._settings.smtp_start_tls:
                client.starttls(context=context)
                client.ehlo()
            if self._settings.smtp_username is not None:
                assert self._settings.smtp_password is not None
                client.login(self._settings.smtp_username, self._settings.smtp_password)
            client.send_message(message)


def identity_email_transport_for(
    settings: IdentityEmailSettings,
    *,
    environment: str,
) -> MemoryIdentityEmailSender | SmtpIdentityEmailSender:
    """@brief 构造已校验 transport 或本地队列 / Build a validated transport or local queue.

    @param settings 身份邮件设置 / Identity-email settings.
    @param environment 部署环境 / Deployment environment.
    @return SMTP transport，或 development/test 内存队列 / SMTP transport or local test queue.
    @raise RuntimeError 部署环境试图使用内存邮件 / Deployed environments request memory mode.
    """

    if settings.mode == "memory":
        if environment not in {"development", "test"}:
            raise RuntimeError(
                "hosted_identity.email.mode must be smtp outside development/test"
            )
        return MemoryIdentityEmailSender()
    return SmtpIdentityEmailSender(settings)


def identity_email_sender_for(
    settings: IdentityEmailSettings,
    *,
    environment: str,
) -> MemoryIdentityEmailSender | SmtpIdentityEmailSender:
    """@brief 兼容旧 composition 名称并转发到 transport factory / Preserve the old factory name.

    @return 由 ``identity_email_transport_for`` 构造的适配器 / Configured adapter.
    """

    return identity_email_transport_for(settings, environment=environment)


__all__ = [
    "MemoryIdentityEmailSender",
    "SmtpIdentityEmailSender",
    "identity_email_sender_for",
    "identity_email_transport_for",
]
