"""Email sending service for authentication."""

from __future__ import annotations

import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import aiosmtplib
from jinja2 import Environment, FileSystemLoader, Template

from derp.auth.config import EmailConfig
from derp.auth.exceptions import EmailSendError


class EmailClient:
    """Async SMTP client for sending emails."""

    def __init__(self, config: EmailConfig) -> None:
        self._config: EmailConfig = config

        path = (
            Path(config.templates_dir)
            if config.templates_dir is not None
            else Path(__file__).resolve().parent / "email_templates"
        )
        self._env = Environment(
            loader=FileSystemLoader(path),
            autoescape=True,
        )
        self._base_template: Template = self._env.get_template("base.html")
        self._confirmation_template: Template = self._env.get_template(
            "confirmation.html"
        )
        self._recovery_template: Template = self._env.get_template("recovery.html")
        self._magic_link_template: Template = self._env.get_template("magic_link.html")

    async def send_email(
        self,
        *,
        subject: str,
        to_email: str,
        html: str,
        text_content: str | None = None,
    ) -> None:
        """Send an email.

        Args:
            to_email: Recipient email address.
            subject: Email subject.
            html_content: HTML body content.
            text_content: Plain text body content (optional fallback).

        Raises:
            EmailSendError: If sending fails
        """
        # Build message
        message = MIMEMultipart("alternative")
        message["From"] = f"{self._config.from_name} <{self._config.from_email}>"
        message["To"] = to_email
        message["Subject"] = subject

        # Add text fallback if provided
        if text_content:
            message.attach(MIMEText(text_content, "plain"))

        # Add HTML content
        message.attach(MIMEText(html, "html"))

        try:
            # Determine SSL/TLS settings
            use_tls = self._config.use_tls
            start_tls = self._config.start_tls

            # Create SSL context if needed
            tls_context = ssl.create_default_context() if use_tls or start_tls else None

            await aiosmtplib.send(
                message,
                hostname=self._config.smtp_host,
                port=self._config.smtp_port,
                username=self._config.smtp_user,
                password=self._config.smtp_password,
                use_tls=use_tls,
                start_tls=start_tls,
                tls_context=tls_context,
            )
        except Exception as e:
            raise EmailSendError(f"Failed to send email: {e}") from e

    async def send_confirmation_email(self, to_email: str, token: str) -> None:
        """Send an email confirmation email."""
        if not self._config.enable_confirmation:
            raise ValueError("Confirmation email is not enabled")

        confirm_url = self._config.confirm_email_url.format(
            site_url=self._config.site_url
        )
        content = self._confirmation_template.render(
            confirm_url=f"{confirm_url}?token={token}",
            site_url=self._config.site_url,
            site_name=self._config.site_name,
        )
        await self.send_email(
            subject="Confirm your email address",
            to_email=to_email,
            html=self._base_template.render(
                title="Confirm your email address",
                content=content,
                site_url=self._config.site_url,
                site_name=self._config.site_name,
            ),
        )

    async def send_recovery_email(self, to_email: str, token: str) -> None:
        """Send a password recovery email."""
        recovery_url = self._config.recovery_url.format(site_url=self._config.site_url)
        content = self._recovery_template.render(
            recovery_url=f"{recovery_url}?token={token}",
            site_url=self._config.site_url,
            site_name=self._config.site_name,
        )
        await self.send_email(
            subject="Reset your password",
            to_email=to_email,
            html=self._base_template.render(
                title="Reset your password",
                content=content,
                site_url=self._config.site_url,
                site_name=self._config.site_name,
            ),
        )

    async def send_magic_link_email(self, to_email: str, token: str) -> None:
        """Send a magic link login email."""
        if not self._config.enable_magic_link:
            raise ValueError("Magic link email is not enabled")

        magic_link_url = self._config.magic_link_url.format(
            site_url=self._config.site_url
        )
        content = self._magic_link_template.render(
            magic_link_url=f"{magic_link_url}?token={token}",
            site_url=self._config.site_url,
            site_name=self._config.site_name,
        )
        await self.send_email(
            subject="Sign in to your account",
            to_email=to_email,
            html=self._base_template.render(
                title="Sign in to your account",
                content=content,
                site_url=self._config.site_url,
                site_name=self._config.site_name,
            ),
        )
