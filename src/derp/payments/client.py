"""Async payments client wrapping Stripe."""

from __future__ import annotations

import inspect
from typing import Any, cast

from etils import epy

from derp.config import PaymentsConfig
from derp.payments.exceptions import (
    PaymentsNotConnectedError,
    PaymentsProviderError,
    WebhookSignatureError,
)
from derp.payments.models import (
    CheckoutSession,
    CheckoutSessionMode,
    Customer,
    StripeListResult,
    WebhookEvent,
)

with epy.lazy_imports():
    import stripe


class PaymentsClient:
    """Async wrapper around Stripe payments APIs."""

    def __init__(self, config: PaymentsConfig):
        self._config = config
        self._stripe_client: stripe.StripeClient | None = None
        self._http_client: stripe.HTTPXClient | None = None

    async def connect(self) -> None:
        """Initialize Stripe client state."""
        if self._stripe_client is not None:
            return

        self._http_client = stripe.HTTPXClient(timeout=self._config.timeout_seconds)
        self._stripe_client = stripe.StripeClient(
            self._config.api_key,
            max_network_retries=self._config.max_network_retries,
            http_client=self._http_client,
        )

    async def disconnect(self) -> None:
        """Clear Stripe client state."""
        if self._http_client is not None:
            close_async = getattr(self._http_client, "close_async", None)
            if callable(close_async):
                maybe_awaitable = close_async()
                if inspect.isawaitable(maybe_awaitable):
                    await maybe_awaitable

        self._stripe_client = None
        self._http_client = None

    async def _provider_call(self, method: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return await method(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - behavior validated via tests
            raise PaymentsProviderError(
                str(exc) or "Payments provider request failed",
                code=str(getattr(exc, "code", "payments_provider_error")),
            ) from exc

    @staticmethod
    def _to_raw(payload: Any) -> dict[str, Any]:
        if payload is None:
            return {}
        to_dict_recursive = getattr(payload, "to_dict_recursive", None)
        if callable(to_dict_recursive):
            maybe_dict = to_dict_recursive()
            if isinstance(maybe_dict, dict):
                return dict(maybe_dict)
        if isinstance(payload, dict):
            return dict(payload)
        values = getattr(payload, "__dict__", None)
        if isinstance(values, dict):
            return dict(values)
        return {}

    @staticmethod
    def _to_metadata(value: Any) -> dict[str, str]:
        if not isinstance(value, dict):
            return {}
        metadata: dict[str, str] = {}
        for key, item in value.items():
            metadata[str(key)] = str(item)
        return metadata

    @staticmethod
    def _customer_id(value: Any) -> str | None:
        if isinstance(value, str):
            return value
        if isinstance(value, dict) and isinstance(value.get("id"), str):
            return cast(str, value["id"])
        return None

    @staticmethod
    def _normalize_customer(raw: dict[str, Any]) -> Customer:
        return Customer(
            id=str(raw.get("id", "")),
            email=cast(str | None, raw.get("email")),
            name=cast(str | None, raw.get("name")),
            phone=cast(str | None, raw.get("phone")),
            metadata=PaymentsClient._to_metadata(raw.get("metadata")),
            created=cast(int | None, raw.get("created")),
            raw=raw,
        )

    @staticmethod
    def _normalize_checkout_session(raw: dict[str, Any]) -> CheckoutSession:
        mode: CheckoutSessionMode | None = None
        if raw.get("mode") in ("payment", "subscription"):
            mode = cast(CheckoutSessionMode, raw["mode"])

        return CheckoutSession(
            id=str(raw.get("id", "")),
            url=cast(str | None, raw.get("url")),
            mode=mode,
            customer_id=PaymentsClient._customer_id(raw.get("customer")),
            customer_email=cast(str | None, raw.get("customer_email")),
            payment_status=cast(str | None, raw.get("payment_status")),
            status=cast(str | None, raw.get("status")),
            expires_at=cast(int | None, raw.get("expires_at")),
            raw=raw,
        )

    @staticmethod
    def _normalize_event(raw: dict[str, Any]) -> WebhookEvent:
        data_object: dict[str, Any] | None = None
        data = raw.get("data")
        if isinstance(data, dict):
            obj = data.get("object")
            if isinstance(obj, dict):
                data_object = obj
            else:
                maybe_object = PaymentsClient._to_raw(obj)
                if maybe_object:
                    data_object = maybe_object

        return WebhookEvent(
            id=str(raw.get("id", "")),
            type=str(raw.get("type", "")),
            created=int(raw.get("created", 0)),
            livemode=bool(raw.get("livemode", False)),
            data_object=data_object,
            raw=raw,
        )

    @staticmethod
    def _normalize_line_items(
        line_items: list[dict[str, str | int]],
    ) -> list[dict[str, str | int]]:
        if not line_items:
            raise ValueError("line_items must contain at least one item.")

        normalized: list[dict[str, str | int]] = []
        for index, item in enumerate(line_items):
            if "price_id" not in item:
                raise ValueError(
                    f"line_items[{index}] must include a 'price_id' field."
                )
            if "quantity" not in item:
                raise ValueError(
                    f"line_items[{index}] must include a 'quantity' field."
                )

            price_id = item["price_id"]
            quantity = item["quantity"]

            if not isinstance(price_id, str) or not price_id.strip():
                raise ValueError(
                    f"line_items[{index}].price_id must be a non-empty str."
                )
            if (
                isinstance(quantity, bool)
                or not isinstance(quantity, int)
                or quantity <= 0
            ):
                raise ValueError(
                    f"line_items[{index}].quantity must be a positive int."
                )

            normalized.append({"price": price_id, "quantity": quantity})

        return normalized

    async def create_customer(
        self,
        *,
        email: str | None = None,
        name: str | None = None,
        phone: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> Customer:
        """Create a Stripe customer."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        params: dict[str, Any] = {}
        if email is not None:
            params["email"] = email
        if name is not None:
            params["name"] = name
        if phone is not None:
            params["phone"] = phone
        if metadata is not None:
            params["metadata"] = metadata

        response = await self._provider_call(
            self._stripe_client.v1.customers.create_async, **params
        )
        return self._normalize_customer(self._to_raw(response))

    async def retrieve_customer(
        self, customer_id: str, *, expand: list[str] | None = None
    ) -> Customer:
        """Retrieve a Stripe customer."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        params: dict[str, Any] = {}
        if expand is not None:
            params["expand"] = expand

        response = await self._provider_call(
            self._stripe_client.v1.customers.retrieve_async,
            customer_id,
            **params,
        )
        return self._normalize_customer(self._to_raw(response))

    async def update_customer(
        self,
        customer_id: str,
        *,
        email: str | None = None,
        name: str | None = None,
        phone: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> Customer:
        """Update a Stripe customer."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        params: dict[str, Any] = {}
        if email is not None:
            params["email"] = email
        if name is not None:
            params["name"] = name
        if phone is not None:
            params["phone"] = phone
        if metadata is not None:
            params["metadata"] = metadata

        response = await self._provider_call(
            self._stripe_client.v1.customers.update_async,
            customer_id,
            **params,
        )
        return self._normalize_customer(self._to_raw(response))

    async def create_checkout_session(
        self,
        *,
        mode: CheckoutSessionMode | str,
        success_url: str,
        cancel_url: str,
        line_items: list[dict[str, str | int]],
        customer_id: str | None = None,
        customer_email: str | None = None,
        client_reference_id: str | None = None,
        metadata: dict[str, str] | None = None,
        allow_promotion_codes: bool | None = None,
        idempotency_key: str | None = None,
    ) -> CheckoutSession:
        """Create a Stripe checkout session."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        if mode not in CheckoutSessionMode:
            raise ValueError(
                f"Invalid mode: {mode}. Must be one of {list(CheckoutSessionMode)}."
            )
        if customer_id is not None and customer_email is not None:
            raise ValueError(
                "Only one of `customer_id` and `customer_email` can be provided,"
                " but both were provided."
            )

        normalized_line_items = self._normalize_line_items(line_items)

        params: dict[str, Any] = {
            "mode": mode,
            "success_url": success_url,
            "cancel_url": cancel_url,
            "line_items": normalized_line_items,
        }

        if customer_id is not None:
            params["customer"] = customer_id
        if customer_email is not None:
            params["customer_email"] = customer_email
        if client_reference_id is not None:
            params["client_reference_id"] = client_reference_id
        if metadata is not None:
            params["metadata"] = metadata
        if allow_promotion_codes is not None:
            params["allow_promotion_codes"] = allow_promotion_codes

        if idempotency_key is not None:
            if not isinstance(idempotency_key, str) or not idempotency_key.strip():
                raise ValueError(
                    "idempotency_key must be a non-empty str when provided."
                )
            params["options"] = {"idempotency_key": idempotency_key}

        response = await self._provider_call(
            self._stripe_client.v1.checkout.sessions.create_async, **params
        )
        return self._normalize_checkout_session(self._to_raw(response))

    async def retrieve_checkout_session(
        self, session_id: str, *, expand: list[str] | None = None
    ) -> CheckoutSession:
        """Retrieve a Stripe checkout session."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        params: dict[str, Any] = {}
        if expand is not None:
            params["expand"] = expand

        response = await self._provider_call(
            self._stripe_client.v1.checkout.sessions.retrieve_async,
            session_id,
            **params,
        )
        return self._normalize_checkout_session(self._to_raw(response))

    async def expire_checkout_session(self, session_id: str) -> CheckoutSession:
        """Expire a Stripe checkout session."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        response = await self._provider_call(
            self._stripe_client.v1.checkout.sessions.expire_async,
            session_id,
        )
        return self._normalize_checkout_session(self._to_raw(response))

    async def _list_resource(
        self,
        list_method: Any,
        *,
        limit: int = 25,
        starting_after: str | None = None,
        expand: list[str] | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> StripeListResult:
        """List Stripe resources with cursor pagination."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        limit = min(max(limit, 1), 100)
        params: dict[str, Any] = {"limit": limit}
        if starting_after is not None:
            params["starting_after"] = starting_after
        if expand is not None:
            params["expand"] = expand
        if extra_params is not None:
            params.update(extra_params)

        result = await self._provider_call(list_method, params)
        data = [self._to_raw(item) for item in result.data]
        return StripeListResult(data=data, has_more=result.has_more)

    async def list_customers(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
    ) -> StripeListResult:
        """List Stripe customers."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        return await self._list_resource(
            self._stripe_client.v1.customers.list_async,
            limit=limit,
            starting_after=starting_after,
        )

    async def list_products(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
    ) -> StripeListResult:
        """List Stripe products with expanded default price."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        return await self._list_resource(
            self._stripe_client.v1.products.list_async,
            limit=limit,
            starting_after=starting_after,
            expand=["data.default_price"],
        )

    async def list_subscriptions(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
    ) -> StripeListResult:
        """List Stripe subscriptions across all statuses."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        return await self._list_resource(
            self._stripe_client.v1.subscriptions.list_async,
            limit=limit,
            starting_after=starting_after,
            extra_params={"status": "all"},
        )

    async def list_invoices(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
    ) -> StripeListResult:
        """List Stripe invoices."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        return await self._list_resource(
            self._stripe_client.v1.invoices.list_async,
            limit=limit,
            starting_after=starting_after,
        )

    async def list_charges(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
    ) -> StripeListResult:
        """List Stripe charges."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        return await self._list_resource(
            self._stripe_client.v1.charges.list_async,
            limit=limit,
            starting_after=starting_after,
        )

    async def verify_webhook_event(
        self,
        *,
        payload: bytes,
        signature: str,
        webhook_secret: str | None = None,
    ) -> WebhookEvent:
        """Verify and parse a Stripe webhook payload."""
        secret = webhook_secret or self._config.webhook_secret
        if not secret:
            raise ValueError(
                "webhook_secret is required. Set PaymentsConfig.webhook_secret "
                "or pass webhook_secret to verify_webhook_event()."
            )

        try:
            event = stripe.Webhook.construct_event(
                payload=payload,
                sig_header=signature,
                secret=secret,
            )
        except Exception as exc:
            raise WebhookSignatureError(
                str(exc) or "Invalid webhook signature"
            ) from exc

        return self._normalize_event(self._to_raw(event))
