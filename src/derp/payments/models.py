"""Typed models for payments client responses."""

from __future__ import annotations

import dataclasses
import enum
from typing import Any


class CheckoutSessionMode(enum.StrEnum):
    """Stripe checkout session mode."""

    PAYMENT = "payment"
    SUBSCRIPTION = "subscription"


@dataclasses.dataclass(slots=True)
class Customer:
    """Normalized Stripe customer payload."""

    id: str
    email: str | None
    name: str | None
    phone: str | None
    metadata: dict[str, str]
    created: int | None
    raw: dict[str, Any]


@dataclasses.dataclass(slots=True)
class CheckoutSession:
    """Normalized Stripe checkout session payload."""

    id: str
    url: str | None
    mode: CheckoutSessionMode | None
    customer_id: str | None
    customer_email: str | None
    payment_status: str | None
    status: str | None
    expires_at: int | None
    raw: dict[str, Any]


@dataclasses.dataclass(slots=True)
class WebhookEvent:
    """Normalized Stripe webhook event payload."""

    id: str
    type: str
    created: int
    livemode: bool
    data_object: dict[str, Any] | None
    raw: dict[str, Any]
