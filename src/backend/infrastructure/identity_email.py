"""Hosted identity email adapters that never log credential material."""

from __future__ import annotations

import asyncio
import smtplib
import ssl
from email.message import EmailMessage

from backend.config import IdentityEmailSettings


class MemoryIdentityEmailSender:
    """Process-local test adapter; production configuration rejects this mode."""

    def __init__(self) -> None:
        self._codes: dict[str, str] = {}
        self.recovery_notifications: list[str] = []

    async def send_verification_code(self, recipient: str, code: str) -> None:
        self._codes[recipient] = code

    async def send_recovery_notification(self, recipient: str) -> None:
        self.recovery_notifications.append(recipient)

    def code_for(self, recipient: str) -> str:
        """Return a code only to in-process automated tests."""

        return self._codes[recipient]


class SmtpIdentityEmailSender:
    """SMTP adapter using STARTTLS and optional authenticated submission."""

    def __init__(self, settings: IdentityEmailSettings) -> None:
        if settings.smtp_host is None or settings.from_address is None:
            raise ValueError("SMTP identity email settings are incomplete")
        self._settings = settings

    async def send_verification_code(self, recipient: str, code: str) -> None:
        message = self._message(
            recipient,
            "Your AI Job Workspace verification code",
            f"Your verification code is {code}. It expires in 10 minutes and can be used once.",
        )
        await asyncio.to_thread(self._send, message)

    async def send_recovery_notification(self, recipient: str) -> None:
        message = self._message(
            recipient,
            "Your AI Job Workspace account was recovered",
            "Your password was changed and existing sessions were revoked. "
            "If this was not you, contact support immediately.",
        )
        await asyncio.to_thread(self._send, message)

    def _message(self, recipient: str, subject: str, body: str) -> EmailMessage:
        message = EmailMessage()
        message["From"] = self._settings.from_address
        message["To"] = recipient
        message["Subject"] = subject
        message.set_content(body)
        return message

    def _send(self, message: EmailMessage) -> None:
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


def identity_email_sender_for(
    settings: IdentityEmailSettings,
    *,
    environment: str,
) -> MemoryIdentityEmailSender | SmtpIdentityEmailSender:
    """Build the configured delivery adapter after configuration fail-closed checks."""

    if settings.mode == "memory":
        if environment not in {"development", "test"}:
            raise RuntimeError(
                "hosted_identity.email.mode must be smtp outside development/test"
            )
        return MemoryIdentityEmailSender()
    return SmtpIdentityEmailSender(settings)


__all__ = [
    "MemoryIdentityEmailSender",
    "SmtpIdentityEmailSender",
    "identity_email_sender_for",
]
