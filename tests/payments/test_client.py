"""Unit tests for payments client behavior."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from derp.config import PaymentsConfig
from derp.payments.client import PaymentsClient
from derp.payments.exceptions import (
    PaymentsNotConnectedError,
    PaymentsProviderError,
    WebhookSignatureError,
)


class StripePayload(dict[str, Any]):
    """Dict-like payload with Stripe-style recursive serializer."""

    def to_dict_recursive(self) -> dict[str, Any]:
        return dict(self)


def _connected_client() -> tuple[PaymentsClient, Any]:
    customers = SimpleNamespace(
        create_async=AsyncMock(),
        retrieve_async=AsyncMock(),
        update_async=AsyncMock(),
    )
    sessions = SimpleNamespace(
        create_async=AsyncMock(),
        retrieve_async=AsyncMock(),
        expire_async=AsyncMock(),
    )
    stripe_client = SimpleNamespace(
        v1=SimpleNamespace(
            customers=customers,
            checkout=SimpleNamespace(sessions=sessions),
        )
    )

    client = PaymentsClient(
        PaymentsConfig(
            api_key="sk_test_123",
            webhook_secret="whsec_test_123",
        )
    )
    client._stripe_client = stripe_client  # type: ignore[assignment]
    return client, stripe_client


@pytest.mark.asyncio
async def test_connect_disconnect_idempotent() -> None:
    fake_http_client = SimpleNamespace(close_async=AsyncMock())
    fake_stripe_client = SimpleNamespace()
    client = PaymentsClient(PaymentsConfig(api_key="sk_test_123"))

    with (
        patch(
            "derp.payments.client.stripe.HTTPXClient", return_value=fake_http_client
        ) as httpx_client,
        patch(
            "derp.payments.client.stripe.StripeClient", return_value=fake_stripe_client
        ) as stripe_client,
    ):
        await client.connect()
        await client.connect()

        assert client._stripe_client is fake_stripe_client
        httpx_client.assert_called_once_with(timeout=30.0)
        stripe_client.assert_called_once()

        await client.disconnect()
        await client.disconnect()

    fake_http_client.close_async.assert_awaited_once()

    with pytest.raises(PaymentsNotConnectedError):
        await client.create_customer(email="user@example.com")


@pytest.mark.asyncio
async def test_create_retrieve_update_customer() -> None:
    client, stripe_client = _connected_client()
    stripe_client.v1.customers.create_async.return_value = StripePayload(
        id="cus_123",
        email="user@example.com",
        name="User",
        phone="+15555550100",
        metadata={"plan": "pro"},
        created=1730000000,
    )
    stripe_client.v1.customers.retrieve_async.return_value = StripePayload(
        id="cus_123",
        email="user@example.com",
        metadata={},
    )
    stripe_client.v1.customers.update_async.return_value = StripePayload(
        id="cus_123",
        email="updated@example.com",
        metadata={"tier": "plus"},
    )

    created = await client.create_customer(
        email="user@example.com",
        name="User",
        phone="+15555550100",
        metadata={"plan": "pro"},
    )
    assert created.id == "cus_123"
    assert created.email == "user@example.com"
    assert created.metadata == {"plan": "pro"}
    stripe_client.v1.customers.create_async.assert_awaited_once_with(
        email="user@example.com",
        name="User",
        phone="+15555550100",
        metadata={"plan": "pro"},
    )

    retrieved = await client.retrieve_customer("cus_123", expand=["subscriptions"])
    assert retrieved.id == "cus_123"
    stripe_client.v1.customers.retrieve_async.assert_awaited_once_with(
        "cus_123",
        expand=["subscriptions"],
    )

    updated = await client.update_customer(
        "cus_123",
        email="updated@example.com",
        metadata={"tier": "plus"},
    )
    assert updated.email == "updated@example.com"
    assert updated.metadata == {"tier": "plus"}
    stripe_client.v1.customers.update_async.assert_awaited_once_with(
        "cus_123",
        email="updated@example.com",
        metadata={"tier": "plus"},
    )


@pytest.mark.asyncio
async def test_customer_provider_error_mapping() -> None:
    client, stripe_client = _connected_client()
    stripe_client.v1.customers.create_async.side_effect = Exception("provider failed")

    with pytest.raises(PaymentsProviderError, match="provider failed"):
        await client.create_customer(email="user@example.com")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["payment", "subscription"])
async def test_create_checkout_session_success(mode: str) -> None:
    client, stripe_client = _connected_client()
    stripe_client.v1.checkout.sessions.create_async.return_value = StripePayload(
        id="cs_123",
        url="https://checkout.stripe.com/pay/cs_123",
        mode=mode,
        customer="cus_123",
        payment_status="unpaid",
        status="open",
        expires_at=1730000001,
    )

    session = await client.create_checkout_session(
        mode=mode,
        success_url="https://app.test/success",
        cancel_url="https://app.test/cancel",
        line_items=[{"price_id": "price_123", "quantity": 2}],
        customer_id="cus_123",
        metadata={"cart_id": "cart_1"},
        allow_promotion_codes=True,
        idempotency_key="checkout-1",
    )

    assert session.id == "cs_123"
    assert session.mode == mode
    assert session.customer_id == "cus_123"
    stripe_client.v1.checkout.sessions.create_async.assert_awaited_once_with(
        mode=mode,
        success_url="https://app.test/success",
        cancel_url="https://app.test/cancel",
        line_items=[{"price": "price_123", "quantity": 2}],
        customer="cus_123",
        metadata={"cart_id": "cart_1"},
        allow_promotion_codes=True,
        options={"idempotency_key": "checkout-1"},
    )


@pytest.mark.asyncio
async def test_checkout_session_retrieve_expire() -> None:
    client, stripe_client = _connected_client()
    stripe_client.v1.checkout.sessions.retrieve_async.return_value = StripePayload(
        id="cs_123",
        mode="payment",
        status="open",
    )
    stripe_client.v1.checkout.sessions.expire_async.return_value = StripePayload(
        id="cs_123",
        mode="payment",
        status="expired",
    )

    retrieved = await client.retrieve_checkout_session("cs_123", expand=["customer"])
    assert retrieved.id == "cs_123"
    assert retrieved.status == "open"
    stripe_client.v1.checkout.sessions.retrieve_async.assert_awaited_once_with(
        "cs_123",
        expand=["customer"],
    )

    expired = await client.expire_checkout_session("cs_123")
    assert expired.status == "expired"
    stripe_client.v1.checkout.sessions.expire_async.assert_awaited_once_with("cs_123")


@pytest.mark.asyncio
async def test_checkout_session_provider_error_mapping() -> None:
    client, stripe_client = _connected_client()
    stripe_client.v1.checkout.sessions.create_async.side_effect = Exception(
        "stripe bad"
    )

    with pytest.raises(PaymentsProviderError, match="stripe bad"):
        await client.create_checkout_session(
            mode="payment",
            success_url="https://app.test/success",
            cancel_url="https://app.test/cancel",
            line_items=[{"price_id": "price_123", "quantity": 1}],
        )


@pytest.mark.asyncio
async def test_checkout_session_validations() -> None:
    client, _ = _connected_client()

    with pytest.raises(ValueError, match="line_items must contain at least one item"):
        await client.create_checkout_session(
            mode="payment",
            success_url="https://app.test/success",
            cancel_url="https://app.test/cancel",
            line_items=[],
        )

    with pytest.raises(ValueError, match="must include a 'price_id'"):
        await client.create_checkout_session(
            mode="payment",
            success_url="https://app.test/success",
            cancel_url="https://app.test/cancel",
            line_items=[{"quantity": 1}],
        )

    with pytest.raises(ValueError, match="must include a 'quantity'"):
        await client.create_checkout_session(
            mode="payment",
            success_url="https://app.test/success",
            cancel_url="https://app.test/cancel",
            line_items=[{"price_id": "price_123"}],
        )

    with pytest.raises(ValueError, match="quantity must be a positive int"):
        await client.create_checkout_session(
            mode="payment",
            success_url="https://app.test/success",
            cancel_url="https://app.test/cancel",
            line_items=[{"price_id": "price_123", "quantity": 0}],
        )

    with pytest.raises(ValueError, match="Only one of `customer_id`"):
        await client.create_checkout_session(
            mode="payment",
            success_url="https://app.test/success",
            cancel_url="https://app.test/cancel",
            line_items=[{"price_id": "price_123", "quantity": 1}],
            customer_id="cus_123",
            customer_email="user@example.com",
        )


@pytest.mark.asyncio
async def test_verify_webhook_event_success_and_failure() -> None:
    client, _ = _connected_client()
    with patch(
        "derp.payments.client.stripe.Webhook.construct_event",
        return_value=StripePayload(
            id="evt_123",
            type="checkout.session.completed",
            created=1730000000,
            livemode=False,
            data={"object": {"id": "cs_123"}},
        ),
    ):
        event = await client.verify_webhook_event(
            payload=b"{}",
            signature="sig",
        )
        assert event.id == "evt_123"
        assert event.type == "checkout.session.completed"
        assert event.data_object == {"id": "cs_123"}

    with patch(
        "derp.payments.client.stripe.Webhook.construct_event",
        side_effect=Exception("bad signature"),
    ):
        with pytest.raises(WebhookSignatureError, match="bad signature"):
            await client.verify_webhook_event(payload=b"{}", signature="sig")


@pytest.mark.asyncio
async def test_verify_webhook_event_requires_secret() -> None:
    client = PaymentsClient(PaymentsConfig(api_key="sk_test_123", webhook_secret=None))

    with pytest.raises(ValueError, match="webhook_secret is required"):
        await client.verify_webhook_event(payload=b"{}", signature="sig")
