"""Unit tests for payments client behavior using stripe-mock."""

from __future__ import annotations

import hashlib
import hmac
import json
import time

import pytest
import stripe as stripe_lib

from derp.config import PaymentsConfig
from derp.payments.client import PaymentsClient
from derp.payments.exceptions import (
    PaymentsNotConnectedError,
    PaymentsProviderError,
    WebhookSignatureError,
)

STRIPE_MOCK_PORT = 12111


@pytest.fixture(autouse=True)
def _require_stripe_mock(stripe_mock: None) -> None:
    """Ensure stripe-mock is running for all tests in this module."""


def _mock_client(*, webhook_secret: str | None = "whsec_test_123") -> PaymentsClient:
    """Create a PaymentsClient wired to local stripe-mock."""
    config = PaymentsConfig(api_key="sk_test_123", webhook_secret=webhook_secret)
    client = PaymentsClient(config)

    http = stripe_lib.HTTPXClient(timeout=30.0)
    sc = stripe_lib.StripeClient(
        "sk_test_123",
        base_addresses={"api": f"http://localhost:{STRIPE_MOCK_PORT}"},
        http_client=http,
    )
    client._stripe_client = sc
    client._http_client = http
    return client


def _broken_client() -> PaymentsClient:
    """Create a PaymentsClient pointed at a dead port for error-path tests."""
    config = PaymentsConfig(api_key="sk_test_123")
    client = PaymentsClient(config)

    http = stripe_lib.HTTPXClient(timeout=0.5)
    sc = stripe_lib.StripeClient(
        "sk_test_123",
        base_addresses={"api": "http://localhost:1"},
        http_client=http,
        max_network_retries=0,
    )
    client._stripe_client = sc
    client._http_client = http
    return client


def _sign_payload(payload: bytes, secret: str) -> str:
    """Generate a valid Stripe-Signature header."""
    timestamp = str(int(time.time()))
    signed = f"{timestamp}.{payload.decode()}"
    sig = hmac.new(secret.encode(), signed.encode(), hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={sig}"


# ---------------------------------------------------------------------------
# Connect / Disconnect
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connect_disconnect_idempotent() -> None:
    client = PaymentsClient(PaymentsConfig(api_key="sk_test_123"))

    await client.connect()
    await client.connect()  # idempotent

    assert client._stripe_client is not None
    assert client._http_client is not None

    await client.disconnect()
    await client.disconnect()  # idempotent

    assert client._stripe_client is None
    assert client._http_client is None

    with pytest.raises(PaymentsNotConnectedError):
        await client.create_customer(email="user@example.com")


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_retrieve_update_customer() -> None:
    client = _mock_client()

    created = await client.create_customer(
        email="user@example.com",
        name="User",
        phone="+15555550100",
        metadata={"plan": "pro"},
    )
    assert created.id
    assert isinstance(created.email, str)
    assert isinstance(created.metadata, dict)
    assert isinstance(created.raw, dict)
    assert created.created is not None

    retrieved = await client.retrieve_customer(created.id, expand=["subscriptions"])
    assert retrieved.id

    updated = await client.update_customer(
        created.id, email="updated@example.com", metadata={"tier": "plus"}
    )
    assert updated.id


@pytest.mark.asyncio
async def test_customer_provider_error_mapping() -> None:
    client = _broken_client()

    with pytest.raises(PaymentsProviderError):
        await client.create_customer(email="user@example.com")


# ---------------------------------------------------------------------------
# Checkout Sessions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["payment", "subscription"])
async def test_create_checkout_session_success(mode: str) -> None:
    client = _mock_client()

    session = await client.create_checkout_session(
        mode=mode,
        success_url="https://app.test/success",
        cancel_url="https://app.test/cancel",
        line_items=[{"price_id": "price_123", "quantity": 2}],
        customer_email="user@example.com",
        metadata={"cart_id": "cart_1"},
        allow_promotion_codes=True,
    )
    assert session.id
    assert isinstance(session.raw, dict)


@pytest.mark.asyncio
async def test_checkout_session_retrieve_expire() -> None:
    client = _mock_client()

    retrieved = await client.retrieve_checkout_session(
        "cs_test_123", expand=["customer"]
    )
    assert retrieved.id
    assert retrieved.status is not None

    expired = await client.expire_checkout_session("cs_test_123")
    assert expired.id


@pytest.mark.asyncio
async def test_checkout_session_provider_error_mapping() -> None:
    client = _broken_client()

    with pytest.raises(PaymentsProviderError):
        await client.create_checkout_session(
            mode="payment",
            success_url="https://app.test/success",
            cancel_url="https://app.test/cancel",
            line_items=[{"price_id": "price_123", "quantity": 1}],
        )


@pytest.mark.asyncio
async def test_checkout_session_validations() -> None:
    client = _mock_client()

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


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verify_webhook_event_success_and_failure() -> None:
    secret = "whsec_test_123"
    client = _mock_client(webhook_secret=secret)

    payload = json.dumps(
        {
            "id": "evt_test_123",
            "object": "event",
            "api_version": "2024-06-20",
            "type": "checkout.session.completed",
            "created": 1730000000,
            "livemode": False,
            "pending_webhooks": 0,
            "request": {"id": None, "idempotency_key": None},
            "data": {"object": {"id": "cs_test_123", "object": "checkout.session"}},
        }
    ).encode()

    signature = _sign_payload(payload, secret)
    event = await client.verify_webhook_event(payload=payload, signature=signature)
    assert event.id == "evt_test_123"
    assert event.type == "checkout.session.completed"
    assert event.data_object is not None
    assert event.data_object["id"] == "cs_test_123"

    with pytest.raises(WebhookSignatureError):
        await client.verify_webhook_event(payload=payload, signature="bad_sig")


@pytest.mark.asyncio
async def test_verify_webhook_event_requires_secret() -> None:
    client = PaymentsClient(PaymentsConfig(api_key="sk_test_123", webhook_secret=None))

    with pytest.raises(ValueError, match="webhook_secret is required"):
        await client.verify_webhook_event(payload=b"{}", signature="sig")


# ---------------------------------------------------------------------------
# Connected Accounts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_retrieve_update_delete_account() -> None:
    client = _mock_client()

    created = await client.create_account(
        type="custom",
        country="US",
        email="seller@example.com",
        metadata={"tier": "basic"},
        capabilities={"card_payments": {"requested": True}},
        business_type="individual",
    )
    assert created.id
    assert isinstance(created.charges_enabled, bool)
    assert isinstance(created.payouts_enabled, bool)
    assert isinstance(created.details_submitted, bool)
    assert isinstance(created.metadata, dict)
    assert isinstance(created.raw, dict)

    retrieved = await client.retrieve_account(created.id)
    assert retrieved.id

    updated = await client.update_account(created.id, metadata={"tier": "pro"})
    assert updated.id

    deleted = await client.delete_account(created.id)
    assert deleted.id


@pytest.mark.asyncio
async def test_create_account_validates_type() -> None:
    client = _mock_client()

    with pytest.raises(ValueError, match="Invalid account type"):
        await client.create_account(type="invalid_type")


@pytest.mark.asyncio
async def test_create_account_link() -> None:
    client = _mock_client()

    link = await client.create_account_link(
        account_id="acct_123",
        refresh_url="https://app.test/refresh",
        return_url="https://app.test/return",
        type="account_onboarding",
    )
    assert link.url
    assert isinstance(link.raw, dict)


@pytest.mark.asyncio
async def test_create_account_link_validates_type() -> None:
    client = _mock_client()

    with pytest.raises(ValueError, match="Invalid account link type"):
        await client.create_account_link(
            account_id="acct_123",
            refresh_url="https://app.test/refresh",
            return_url="https://app.test/return",
            type="bad_type",
        )


@pytest.mark.asyncio
async def test_list_accounts() -> None:
    client = _mock_client()

    result = await client.list_accounts(limit=10)
    assert isinstance(result.data, list)
    assert isinstance(result.has_more, bool)


@pytest.mark.asyncio
async def test_account_provider_error_mapping() -> None:
    client = _broken_client()

    with pytest.raises(PaymentsProviderError):
        await client.create_account(type="express")


# ---------------------------------------------------------------------------
# Payment Intents
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_payment_intent_full_params() -> None:
    client = _mock_client()

    intent = await client.create_payment_intent(
        amount=5000,
        currency="usd",
        customer_id="cus_123",
        description="Order 789",
        capture_method="manual",
        payment_method_types=["card"],
        transfer_data={"destination": "acct_456"},
        application_fee_amount=500,
        on_behalf_of="acct_456",
        metadata={"order_id": "789"},
    )
    assert intent.id
    assert isinstance(intent.amount, int)
    assert isinstance(intent.currency, str)
    assert isinstance(intent.metadata, dict)
    assert isinstance(intent.raw, dict)


@pytest.mark.asyncio
async def test_create_payment_intent_validates_amount() -> None:
    client = _mock_client()

    with pytest.raises(ValueError, match="amount must be a positive integer"):
        await client.create_payment_intent(amount=0, currency="usd")

    with pytest.raises(ValueError, match="amount must be a positive integer"):
        await client.create_payment_intent(amount=-100, currency="usd")


@pytest.mark.asyncio
async def test_create_payment_intent_validates_capture_method() -> None:
    client = _mock_client()

    with pytest.raises(ValueError, match="Invalid capture_method"):
        await client.create_payment_intent(
            amount=1000, currency="usd", capture_method="invalid"
        )


@pytest.mark.asyncio
async def test_retrieve_payment_intent_with_expand() -> None:
    client = _mock_client()

    intent = await client.retrieve_payment_intent("pi_test_123", expand=["charges"])
    assert intent.id
    assert intent.status is not None


@pytest.mark.asyncio
async def test_confirm_payment_intent() -> None:
    client = _mock_client()

    intent = await client.confirm_payment_intent(
        "pi_test_123", payment_method="pm_card_visa"
    )
    assert intent.id


@pytest.mark.asyncio
async def test_capture_payment_intent() -> None:
    client = _mock_client()

    intent = await client.capture_payment_intent("pi_test_123", amount_to_capture=3000)
    assert intent.id


@pytest.mark.asyncio
async def test_cancel_payment_intent_with_reason() -> None:
    client = _mock_client()

    intent = await client.cancel_payment_intent(
        "pi_test_123", cancellation_reason="requested_by_customer"
    )
    assert intent.id


@pytest.mark.asyncio
async def test_cancel_payment_intent_validates_reason() -> None:
    client = _mock_client()

    with pytest.raises(ValueError, match="Invalid cancellation_reason"):
        await client.cancel_payment_intent("pi_123", cancellation_reason="nope")


@pytest.mark.asyncio
async def test_list_payment_intents_with_customer() -> None:
    client = _mock_client()

    result = await client.list_payment_intents(customer_id="cus_123", limit=2)
    assert isinstance(result.data, list)
    assert isinstance(result.has_more, bool)


@pytest.mark.asyncio
async def test_payment_intent_provider_error_mapping() -> None:
    client = _broken_client()

    with pytest.raises(PaymentsProviderError):
        await client.create_payment_intent(amount=1000, currency="usd")


# ---------------------------------------------------------------------------
# Refunds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_refund_with_payment_intent() -> None:
    client = _mock_client()

    refund = await client.create_refund(
        payment_intent_id="pi_test_123",
        reason="requested_by_customer",
        reverse_transfer=True,
        refund_application_fee=True,
        metadata={"note": "damaged"},
    )
    assert refund.id
    assert isinstance(refund.amount, int)
    assert isinstance(refund.metadata, dict)


@pytest.mark.asyncio
async def test_create_refund_with_charge() -> None:
    client = _mock_client()

    refund = await client.create_refund(charge_id="ch_test_123", amount=2000)
    assert refund.id


@pytest.mark.asyncio
async def test_create_refund_requires_payment_intent_or_charge() -> None:
    client = _mock_client()

    with pytest.raises(
        ValueError, match="At least one of `payment_intent_id` or `charge_id`"
    ):
        await client.create_refund()


@pytest.mark.asyncio
async def test_create_refund_validates_reason() -> None:
    client = _mock_client()

    with pytest.raises(ValueError, match="Invalid refund reason"):
        await client.create_refund(payment_intent_id="pi_123", reason="bad_reason")


@pytest.mark.asyncio
async def test_retrieve_refund() -> None:
    client = _mock_client()

    refund = await client.retrieve_refund("re_test_123")
    assert refund.id


@pytest.mark.asyncio
async def test_list_refunds_with_filters() -> None:
    client = _mock_client()

    result = await client.list_refunds(payment_intent_id="pi_test_123", limit=5)
    assert isinstance(result.data, list)
    assert isinstance(result.has_more, bool)


# ---------------------------------------------------------------------------
# Transfers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_retrieve_transfer() -> None:
    client = _mock_client()

    transfer = await client.create_transfer(
        amount=4500,
        currency="usd",
        destination="acct_test_123",
        description="Order payout",
        transfer_group="order_789",
        metadata={"order_id": "789"},
    )
    assert transfer.id
    assert isinstance(transfer.amount, int)
    assert isinstance(transfer.metadata, dict)

    retrieved = await client.retrieve_transfer(transfer.id)
    assert retrieved.id


@pytest.mark.asyncio
async def test_create_transfer_validates_amount() -> None:
    client = _mock_client()

    with pytest.raises(ValueError, match="amount must be a positive integer"):
        await client.create_transfer(amount=0, currency="usd", destination="acct_456")


@pytest.mark.asyncio
async def test_list_transfers_with_filters() -> None:
    client = _mock_client()

    result = await client.list_transfers(
        destination="acct_test_123", transfer_group="order_789"
    )
    assert isinstance(result.data, list)


@pytest.mark.asyncio
async def test_transfer_provider_error_mapping() -> None:
    client = _broken_client()

    with pytest.raises(PaymentsProviderError):
        await client.create_transfer(
            amount=1000, currency="usd", destination="acct_456"
        )


# ---------------------------------------------------------------------------
# Payouts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_payout_with_stripe_account() -> None:
    client = _mock_client()

    payout = await client.create_payout(
        amount=10000,
        currency="usd",
        method="instant",
        stripe_account="acct_test_123",
    )
    assert payout.id
    assert isinstance(payout.amount, int)
    assert isinstance(payout.metadata, dict)


@pytest.mark.asyncio
async def test_create_payout_validates_amount() -> None:
    client = _mock_client()

    with pytest.raises(ValueError, match="amount must be a positive integer"):
        await client.create_payout(amount=0, currency="usd")


@pytest.mark.asyncio
async def test_create_payout_validates_method() -> None:
    client = _mock_client()

    with pytest.raises(ValueError, match="Invalid payout method"):
        await client.create_payout(amount=1000, currency="usd", method="wire")


@pytest.mark.asyncio
async def test_retrieve_payout_with_stripe_account() -> None:
    client = _mock_client()

    payout = await client.retrieve_payout("po_test_123", stripe_account="acct_test_123")
    assert payout.id


@pytest.mark.asyncio
async def test_cancel_payout() -> None:
    client = _mock_client()

    payout = await client.cancel_payout("po_test_123", stripe_account="acct_test_123")
    assert payout.id


@pytest.mark.asyncio
async def test_list_payouts_with_stripe_account() -> None:
    client = _mock_client()

    result = await client.list_payouts(stripe_account="acct_test_123", limit=10)
    assert isinstance(result.data, list)


# ---------------------------------------------------------------------------
# Balance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retrieve_balance() -> None:
    client = _mock_client()

    balance = await client.retrieve_balance()
    assert isinstance(balance.available, list)
    assert isinstance(balance.pending, list)
    assert isinstance(balance.raw, dict)


@pytest.mark.asyncio
async def test_retrieve_balance_with_stripe_account() -> None:
    client = _mock_client()

    balance = await client.retrieve_balance(stripe_account="acct_test_123")
    assert isinstance(balance.available, list)
    assert isinstance(balance.pending, list)
