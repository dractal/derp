"""Email sending service for authentication."""

from __future__ import annotations

import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import aiosmtplib
from jinja2 import Environment, FileSystemLoader, Template

from derp.auth.exceptions import EmailSendError
from derp.config import EmailConfig


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

        templates: dict[str, Template] = {}
        for template in self._env.list_templates():
            templates[template] = self._env.get_template(template)
        self._templates: dict[str, Template] = templates
        self._base_template: Template | None = templates.get("base.html", None)

        # Create SSL context if needed
        self._ssl_context = (
            ssl.create_default_context()
            if self._config.use_tls or self._config.start_tls
            else None
        )

    async def send_email(
        self,
        *,
        subject: str,
        to_email: str,
        template: str,
        fallback_text: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Send an email.

        Args:
            subject: Email subject.
            to_email: Recipient email address.
            template: Template name.
            fallback_text: Fallback text content.
            **kwargs: Template variables.

        Raises:
            EmailSendError: If sending fails
        """
        if template not in self._templates:
            raise ValueError(
                f"Template '{template}' not found. Available templates: "
                f"{list(self._templates)}."
            )

        # Build message
        message = MIMEMultipart("alternative")
        message["From"] = self._config.from_email
        message["To"] = to_email
        message["Subject"] = subject

        # Add text fallback if provided
        if fallback_text:
            message.attach(MIMEText(fallback_text, "plain"))

        # Add HTML content
        content = self._templates[template].render(**kwargs)
        if self._base_template is not None:
            content = self._base_template.render(
                subject=subject,
                content=content,
                site_url=self._config.site_url,
                site_name=self._config.site_name,
            )
        message.attach(MIMEText(content, "html"))

        try:
            await aiosmtplib.send(
                message,
                hostname=self._config.smtp_host,
                port=self._config.smtp_port,
                username=self._config.smtp_user,
                password=self._config.smtp_password,
                use_tls=self._config.use_tls,
                start_tls=self._config.start_tls,
                tls_context=self._ssl_context,
            )
        except Exception as e:
            raise EmailSendError(f"Failed to send email: {e}") from e

    # async def send_confirmation_email(self, to_email: str, token: str) -> None:
    #     """Send an email confirmation email."""
    #     if not self._config.enable_confirmation:
    #         raise ValueError("Confirmation email is not enabled")

    #     confirm_url = self._config.confirm_email_url.format(
    #         site_url=self._config.site_url
    #     )
    #     await self.send_email(
    #         subject="Confirm your email address",
    #         to_email=to_email,
    #         template="confirmation.html",
    #         confirm_url=f"{confirm_url}?token={token}",
    #         site_url=self._config.site_url,
    #         site_name=self._config.site_name,
    #     )

    # async def send_recovery_email(self, to_email: str, token: str) -> None:
    #     """Send a password recovery email."""
    #     recovery_url=self._config.recovery_url.format(site_url=self._config.site_url)
    #     await self.send_email(
    #         subject="Reset your password",
    #         to_email=to_email,
    #         template="recovery.html",
    #         recovery_url=f"{recovery_url}?token={token}",
    #         site_url=self._config.site_url,
    #         site_name=self._config.site_name,
    #     )

    # async def send_magic_link_email(self, to_email: str, token: str) -> None:
    #     """Send a magic link login email."""
    #     if not self._config.enable_magic_link:
    #         raise ValueError("Magic link email is not enabled")

    #     magic_link_url = self._config.magic_link_url.format(
    #         site_url=self._config.site_url
    #     )
    #     await self.send_email(
    #         subject="Sign in to your account",
    #         to_email=to_email,
    #         template="magic_link.html",
    #         magic_link_url=f"{magic_link_url}?token={token}",
    #         site_url=self._config.site_url,
    #         site_name=self._config.site_name,
    #     )
