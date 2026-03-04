"""Typed models for payments client responses."""

from __future__ import annotations

import dataclasses
import enum
from typing import Any


class CheckoutSessionMode(enum.StrEnum):
    """Stripe checkout session mode."""

    PAYMENT = "payment"
    SUBSCRIPTION = "subscription"


class AccountType(enum.StrEnum):
    """Stripe Connect account type."""

    STANDARD = "standard"
    EXPRESS = "express"
    CUSTOM = "custom"


class AccountLinkType(enum.StrEnum):
    """Stripe account link type."""

    ACCOUNT_ONBOARDING = "account_onboarding"
    ACCOUNT_UPDATE = "account_update"


class CaptureMethod(enum.StrEnum):
    """Payment intent capture method."""

    AUTOMATIC = "automatic"
    AUTOMATIC_ASYNC = "automatic_async"
    MANUAL = "manual"


class CancellationReason(enum.StrEnum):
    """Payment intent cancellation reason."""

    DUPLICATE = "duplicate"
    FRAUDULENT = "fraudulent"
    REQUESTED_BY_CUSTOMER = "requested_by_customer"
    ABANDONED = "abandoned"


class RefundReason(enum.StrEnum):
    """Refund reason."""

    DUPLICATE = "duplicate"
    FRAUDULENT = "fraudulent"
    REQUESTED_BY_CUSTOMER = "requested_by_customer"


class PayoutMethod(enum.StrEnum):
    """Payout method."""

    STANDARD = "standard"
    INSTANT = "instant"


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


@dataclasses.dataclass(slots=True)
class StripeListResult:
    """Paginated list of raw Stripe objects."""

    data: list[dict[str, Any]]
    has_more: bool


@dataclasses.dataclass(slots=True)
class Account:
    """Normalized Stripe Connect account."""

    id: str
    type: str | None
    email: str | None
    country: str | None
    charges_enabled: bool
    payouts_enabled: bool
    details_submitted: bool
    business_type: str | None
    metadata: dict[str, str]
    created: int | None
    raw: dict[str, Any]


@dataclasses.dataclass(slots=True)
class AccountLink:
    """Normalized Stripe account link."""

    url: str
    created: int | None
    expires_at: int | None
    raw: dict[str, Any]


@dataclasses.dataclass(slots=True)
class Transfer:
    """Normalized Stripe transfer."""

    id: str
    amount: int
    currency: str
    destination: str | None
    description: str | None
    transfer_group: str | None
    metadata: dict[str, str]
    created: int | None
    raw: dict[str, Any]


@dataclasses.dataclass(slots=True)
class PaymentIntent:
    """Normalized Stripe payment intent."""

    id: str
    amount: int
    currency: str
    status: str | None
    customer_id: str | None
    description: str | None
    capture_method: str | None
    cancellation_reason: str | None
    payment_method: str | None
    metadata: dict[str, str]
    created: int | None
    raw: dict[str, Any]


@dataclasses.dataclass(slots=True)
class Refund:
    """Normalized Stripe refund."""

    id: str
    amount: int
    currency: str
    status: str | None
    payment_intent_id: str | None
    charge_id: str | None
    reason: str | None
    metadata: dict[str, str]
    created: int | None
    raw: dict[str, Any]


@dataclasses.dataclass(slots=True)
class Payout:
    """Normalized Stripe payout."""

    id: str
    amount: int
    currency: str
    status: str | None
    method: str | None
    description: str | None
    destination: str | None
    metadata: dict[str, str]
    arrival_date: int | None
    created: int | None
    raw: dict[str, Any]


@dataclasses.dataclass(slots=True)
class Balance:
    """Normalized Stripe balance."""

    available: list[dict[str, Any]]
    pending: list[dict[str, Any]]
    connect_reserved: list[dict[str, Any]] | None
    raw: dict[str, Any]
