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
    Account,
    AccountLink,
    AccountLinkType,
    AccountType,
    Balance,
    CancellationReason,
    CaptureMethod,
    CheckoutSession,
    CheckoutSessionMode,
    Customer,
    PaymentIntent,
    Payout,
    PayoutMethod,
    Refund,
    RefundReason,
    StripeListResult,
    Transfer,
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
        except Exception as exc:
            raise PaymentsProviderError(
                str(exc) or "Payments provider request failed",
                code=str(getattr(exc, "code", "payments_provider_error")),
            ) from exc

    @staticmethod
    def _to_raw(payload: Any) -> dict[str, Any]:
        if payload is None:
            return {}
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
    def _connect_options(stripe_account: str | None) -> dict[str, Any] | None:
        if stripe_account is None:
            return None
        return {"stripe_account": stripe_account}

    @staticmethod
    def _normalize_account(raw: dict[str, Any]) -> Account:
        return Account(
            id=str(raw.get("id", "")),
            type=cast(str | None, raw.get("type")),
            email=cast(str | None, raw.get("email")),
            country=cast(str | None, raw.get("country")),
            charges_enabled=bool(raw.get("charges_enabled", False)),
            payouts_enabled=bool(raw.get("payouts_enabled", False)),
            details_submitted=bool(raw.get("details_submitted", False)),
            business_type=cast(str | None, raw.get("business_type")),
            metadata=PaymentsClient._to_metadata(raw.get("metadata")),
            created=cast(int | None, raw.get("created")),
            raw=raw,
        )

    @staticmethod
    def _normalize_account_link(raw: dict[str, Any]) -> AccountLink:
        return AccountLink(
            url=str(raw.get("url", "")),
            created=cast(int | None, raw.get("created")),
            expires_at=cast(int | None, raw.get("expires_at")),
            raw=raw,
        )

    @staticmethod
    def _normalize_transfer(raw: dict[str, Any]) -> Transfer:
        return Transfer(
            id=str(raw.get("id", "")),
            amount=int(raw.get("amount", 0)),
            currency=str(raw.get("currency", "")),
            destination=cast(str | None, raw.get("destination")),
            description=cast(str | None, raw.get("description")),
            transfer_group=cast(str | None, raw.get("transfer_group")),
            metadata=PaymentsClient._to_metadata(raw.get("metadata")),
            created=cast(int | None, raw.get("created")),
            raw=raw,
        )

    @staticmethod
    def _normalize_payment_intent(raw: dict[str, Any]) -> PaymentIntent:
        return PaymentIntent(
            id=str(raw.get("id", "")),
            amount=int(raw.get("amount", 0)),
            currency=str(raw.get("currency", "")),
            status=cast(str | None, raw.get("status")),
            customer_id=PaymentsClient._customer_id(raw.get("customer")),
            description=cast(str | None, raw.get("description")),
            capture_method=cast(str | None, raw.get("capture_method")),
            cancellation_reason=cast(str | None, raw.get("cancellation_reason")),
            payment_method=cast(str | None, raw.get("payment_method")),
            metadata=PaymentsClient._to_metadata(raw.get("metadata")),
            created=cast(int | None, raw.get("created")),
            raw=raw,
        )

    @staticmethod
    def _normalize_refund(raw: dict[str, Any]) -> Refund:
        return Refund(
            id=str(raw.get("id", "")),
            amount=int(raw.get("amount", 0)),
            currency=str(raw.get("currency", "")),
            status=cast(str | None, raw.get("status")),
            payment_intent_id=cast(str | None, raw.get("payment_intent")),
            charge_id=cast(str | None, raw.get("charge")),
            reason=cast(str | None, raw.get("reason")),
            metadata=PaymentsClient._to_metadata(raw.get("metadata")),
            created=cast(int | None, raw.get("created")),
            raw=raw,
        )

    @staticmethod
    def _normalize_payout(raw: dict[str, Any]) -> Payout:
        return Payout(
            id=str(raw.get("id", "")),
            amount=int(raw.get("amount", 0)),
            currency=str(raw.get("currency", "")),
            status=cast(str | None, raw.get("status")),
            method=cast(str | None, raw.get("method")),
            description=cast(str | None, raw.get("description")),
            destination=cast(str | None, raw.get("destination")),
            metadata=PaymentsClient._to_metadata(raw.get("metadata")),
            arrival_date=cast(int | None, raw.get("arrival_date")),
            created=cast(int | None, raw.get("created")),
            raw=raw,
        )

    @staticmethod
    def _normalize_balance(raw: dict[str, Any]) -> Balance:
        available = raw.get("available")
        pending = raw.get("pending")
        connect_reserved = raw.get("connect_reserved")
        return Balance(
            available=list(available) if isinstance(available, list) else [],
            pending=list(pending) if isinstance(pending, list) else [],
            connect_reserved=(
                list(connect_reserved) if isinstance(connect_reserved, list) else None
            ),
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
            self._stripe_client.v1.customers.create_async, params
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
            params,
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
            params,
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

        options: dict[str, Any] | None = None
        if idempotency_key is not None:
            if not isinstance(idempotency_key, str) or not idempotency_key.strip():
                raise ValueError(
                    "idempotency_key must be a non-empty str when provided."
                )
            options = {"idempotency_key": idempotency_key}

        args: list[Any] = [params]
        if options is not None:
            args.append(options)

        response = await self._provider_call(
            self._stripe_client.v1.checkout.sessions.create_async, *args
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
            params,
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
        options: dict[str, Any] | None = None,
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

        if options is not None:
            result = await self._provider_call(list_method, params, options)
        else:
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

    # ------------------------------------------------------------------
    # Connected Accounts
    # ------------------------------------------------------------------

    async def create_account(
        self,
        *,
        type: AccountType | str | None = None,
        country: str | None = None,
        email: str | None = None,
        metadata: dict[str, str] | None = None,
        capabilities: dict[str, dict[str, bool]] | None = None,
        business_type: str | None = None,
        business_profile: dict[str, Any] | None = None,
    ) -> Account:
        """Create a Stripe Connect account."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        if type is not None and type not in AccountType:
            raise ValueError(
                f"Invalid account type: {type}. Must be one of {list(AccountType)}."
            )

        params: dict[str, Any] = {}
        if type is not None:
            params["type"] = type
        if country is not None:
            params["country"] = country
        if email is not None:
            params["email"] = email
        if metadata is not None:
            params["metadata"] = metadata
        if capabilities is not None:
            params["capabilities"] = capabilities
        if business_type is not None:
            params["business_type"] = business_type
        if business_profile is not None:
            params["business_profile"] = business_profile

        response = await self._provider_call(
            self._stripe_client.v1.accounts.create_async, params
        )
        return self._normalize_account(self._to_raw(response))

    async def retrieve_account(self, account_id: str) -> Account:
        """Retrieve a Stripe Connect account."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        response = await self._provider_call(
            self._stripe_client.v1.accounts.retrieve_async, account_id
        )
        return self._normalize_account(self._to_raw(response))

    async def update_account(
        self,
        account_id: str,
        *,
        metadata: dict[str, str] | None = None,
        business_profile: dict[str, Any] | None = None,
    ) -> Account:
        """Update a Stripe Connect account."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        params: dict[str, Any] = {}
        if metadata is not None:
            params["metadata"] = metadata
        if business_profile is not None:
            params["business_profile"] = business_profile

        response = await self._provider_call(
            self._stripe_client.v1.accounts.update_async, account_id, params
        )
        return self._normalize_account(self._to_raw(response))

    async def delete_account(self, account_id: str) -> Account:
        """Delete a Stripe Connect account."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        response = await self._provider_call(
            self._stripe_client.v1.accounts.delete_async, account_id
        )
        return self._normalize_account(self._to_raw(response))

    async def create_account_link(
        self,
        *,
        account_id: str,
        refresh_url: str,
        return_url: str,
        type: AccountLinkType | str,
    ) -> AccountLink:
        """Create a Stripe account onboarding or update link."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        if type not in AccountLinkType:
            raise ValueError(
                f"Invalid account link type: {type}. "
                f"Must be one of {list(AccountLinkType)}."
            )

        response = await self._provider_call(
            self._stripe_client.v1.account_links.create_async,
            {
                "account": account_id,
                "refresh_url": refresh_url,
                "return_url": return_url,
                "type": type,
            },
        )
        return self._normalize_account_link(self._to_raw(response))

    async def list_accounts(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
    ) -> StripeListResult:
        """List Stripe Connect accounts."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        return await self._list_resource(
            self._stripe_client.v1.accounts.list_async,
            limit=limit,
            starting_after=starting_after,
        )

    # ------------------------------------------------------------------
    # Transfers
    # ------------------------------------------------------------------

    async def create_transfer(
        self,
        *,
        amount: int,
        currency: str,
        destination: str,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        transfer_group: str | None = None,
    ) -> Transfer:
        """Create a Stripe transfer to a connected account."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
            raise ValueError("amount must be a positive integer.")

        params: dict[str, Any] = {
            "amount": amount,
            "currency": currency,
            "destination": destination,
        }
        if description is not None:
            params["description"] = description
        if metadata is not None:
            params["metadata"] = metadata
        if transfer_group is not None:
            params["transfer_group"] = transfer_group

        response = await self._provider_call(
            self._stripe_client.v1.transfers.create_async, params
        )
        return self._normalize_transfer(self._to_raw(response))

    async def retrieve_transfer(self, transfer_id: str) -> Transfer:
        """Retrieve a Stripe transfer."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        response = await self._provider_call(
            self._stripe_client.v1.transfers.retrieve_async, transfer_id
        )
        return self._normalize_transfer(self._to_raw(response))

    async def list_transfers(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
        destination: str | None = None,
        transfer_group: str | None = None,
    ) -> StripeListResult:
        """List Stripe transfers."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        extra: dict[str, Any] = {}
        if destination is not None:
            extra["destination"] = destination
        if transfer_group is not None:
            extra["transfer_group"] = transfer_group

        return await self._list_resource(
            self._stripe_client.v1.transfers.list_async,
            limit=limit,
            starting_after=starting_after,
            extra_params=extra or None,
        )

    # ------------------------------------------------------------------
    # Payment Intents
    # ------------------------------------------------------------------

    async def create_payment_intent(
        self,
        *,
        amount: int,
        currency: str,
        customer_id: str | None = None,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        payment_method_types: list[str] | None = None,
        capture_method: CaptureMethod | str | None = None,
        transfer_data: dict[str, Any] | None = None,
        application_fee_amount: int | None = None,
        on_behalf_of: str | None = None,
    ) -> PaymentIntent:
        """Create a Stripe payment intent."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
            raise ValueError("amount must be a positive integer.")
        if capture_method is not None and capture_method not in CaptureMethod:
            raise ValueError(
                f"Invalid capture_method: {capture_method}. "
                f"Must be one of {list(CaptureMethod)}."
            )

        params: dict[str, Any] = {"amount": amount, "currency": currency}
        if customer_id is not None:
            params["customer"] = customer_id
        if metadata is not None:
            params["metadata"] = metadata
        if description is not None:
            params["description"] = description
        if payment_method_types is not None:
            params["payment_method_types"] = payment_method_types
        if capture_method is not None:
            params["capture_method"] = capture_method
        if transfer_data is not None:
            params["transfer_data"] = transfer_data
        if application_fee_amount is not None:
            params["application_fee_amount"] = application_fee_amount
        if on_behalf_of is not None:
            params["on_behalf_of"] = on_behalf_of

        response = await self._provider_call(
            self._stripe_client.v1.payment_intents.create_async, params
        )
        return self._normalize_payment_intent(self._to_raw(response))

    async def retrieve_payment_intent(
        self, payment_intent_id: str, *, expand: list[str] | None = None
    ) -> PaymentIntent:
        """Retrieve a Stripe payment intent."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        params: dict[str, Any] = {}
        if expand is not None:
            params["expand"] = expand

        response = await self._provider_call(
            self._stripe_client.v1.payment_intents.retrieve_async,
            payment_intent_id,
            params,
        )
        return self._normalize_payment_intent(self._to_raw(response))

    async def confirm_payment_intent(
        self, payment_intent_id: str, *, payment_method: str | None = None
    ) -> PaymentIntent:
        """Confirm a Stripe payment intent."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        params: dict[str, Any] = {}
        if payment_method is not None:
            params["payment_method"] = payment_method

        response = await self._provider_call(
            self._stripe_client.v1.payment_intents.confirm_async,
            payment_intent_id,
            params,
        )
        return self._normalize_payment_intent(self._to_raw(response))

    async def capture_payment_intent(
        self, payment_intent_id: str, *, amount_to_capture: int | None = None
    ) -> PaymentIntent:
        """Capture an authorized Stripe payment intent."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        params: dict[str, Any] = {}
        if amount_to_capture is not None:
            params["amount_to_capture"] = amount_to_capture

        response = await self._provider_call(
            self._stripe_client.v1.payment_intents.capture_async,
            payment_intent_id,
            params,
        )
        return self._normalize_payment_intent(self._to_raw(response))

    async def cancel_payment_intent(
        self,
        payment_intent_id: str,
        *,
        cancellation_reason: CancellationReason | str | None = None,
    ) -> PaymentIntent:
        """Cancel a Stripe payment intent."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        if (
            cancellation_reason is not None
            and cancellation_reason not in CancellationReason
        ):
            raise ValueError(
                f"Invalid cancellation_reason: {cancellation_reason}. "
                f"Must be one of {list(CancellationReason)}."
            )

        params: dict[str, Any] = {}
        if cancellation_reason is not None:
            params["cancellation_reason"] = cancellation_reason

        response = await self._provider_call(
            self._stripe_client.v1.payment_intents.cancel_async,
            payment_intent_id,
            params,
        )
        return self._normalize_payment_intent(self._to_raw(response))

    async def list_payment_intents(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
        customer_id: str | None = None,
    ) -> StripeListResult:
        """List Stripe payment intents."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        extra: dict[str, Any] = {}
        if customer_id is not None:
            extra["customer"] = customer_id

        return await self._list_resource(
            self._stripe_client.v1.payment_intents.list_async,
            limit=limit,
            starting_after=starting_after,
            extra_params=extra or None,
        )

    # ------------------------------------------------------------------
    # Refunds
    # ------------------------------------------------------------------

    async def create_refund(
        self,
        *,
        payment_intent_id: str | None = None,
        charge_id: str | None = None,
        amount: int | None = None,
        reason: RefundReason | str | None = None,
        metadata: dict[str, str] | None = None,
        reverse_transfer: bool | None = None,
        refund_application_fee: bool | None = None,
    ) -> Refund:
        """Create a Stripe refund."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        if payment_intent_id is None and charge_id is None:
            raise ValueError(
                "At least one of `payment_intent_id` or `charge_id` must be provided."
            )
        if reason is not None and reason not in RefundReason:
            raise ValueError(
                f"Invalid refund reason: {reason}. Must be one of {list(RefundReason)}."
            )

        params: dict[str, Any] = {}
        if payment_intent_id is not None:
            params["payment_intent"] = payment_intent_id
        if charge_id is not None:
            params["charge"] = charge_id
        if amount is not None:
            params["amount"] = amount
        if reason is not None:
            params["reason"] = reason
        if metadata is not None:
            params["metadata"] = metadata
        if reverse_transfer is not None:
            params["reverse_transfer"] = reverse_transfer
        if refund_application_fee is not None:
            params["refund_application_fee"] = refund_application_fee

        response = await self._provider_call(
            self._stripe_client.v1.refunds.create_async, params
        )
        return self._normalize_refund(self._to_raw(response))

    async def retrieve_refund(self, refund_id: str) -> Refund:
        """Retrieve a Stripe refund."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        response = await self._provider_call(
            self._stripe_client.v1.refunds.retrieve_async, refund_id
        )
        return self._normalize_refund(self._to_raw(response))

    async def list_refunds(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
        payment_intent_id: str | None = None,
        charge_id: str | None = None,
    ) -> StripeListResult:
        """List Stripe refunds."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        extra: dict[str, Any] = {}
        if payment_intent_id is not None:
            extra["payment_intent"] = payment_intent_id
        if charge_id is not None:
            extra["charge"] = charge_id

        return await self._list_resource(
            self._stripe_client.v1.refunds.list_async,
            limit=limit,
            starting_after=starting_after,
            extra_params=extra or None,
        )

    # ------------------------------------------------------------------
    # Payouts
    # ------------------------------------------------------------------

    async def create_payout(
        self,
        *,
        amount: int,
        currency: str,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        destination: str | None = None,
        method: PayoutMethod | str | None = None,
        stripe_account: str | None = None,
    ) -> Payout:
        """Create a Stripe payout."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
            raise ValueError("amount must be a positive integer.")
        if method is not None and method not in PayoutMethod:
            raise ValueError(
                f"Invalid payout method: {method}. Must be one of {list(PayoutMethod)}."
            )

        params: dict[str, Any] = {"amount": amount, "currency": currency}
        if description is not None:
            params["description"] = description
        if metadata is not None:
            params["metadata"] = metadata
        if destination is not None:
            params["destination"] = destination
        if method is not None:
            params["method"] = method

        options = self._connect_options(stripe_account)
        args: list[Any] = [params]
        if options is not None:
            args.append(options)

        response = await self._provider_call(
            self._stripe_client.v1.payouts.create_async, *args
        )
        return self._normalize_payout(self._to_raw(response))

    async def retrieve_payout(
        self, payout_id: str, *, stripe_account: str | None = None
    ) -> Payout:
        """Retrieve a Stripe payout."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        options = self._connect_options(stripe_account)
        response = await self._provider_call(
            self._stripe_client.v1.payouts.retrieve_async,
            payout_id,
            None,
            options,
        )
        return self._normalize_payout(self._to_raw(response))

    async def cancel_payout(
        self, payout_id: str, *, stripe_account: str | None = None
    ) -> Payout:
        """Cancel a pending Stripe payout."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        options = self._connect_options(stripe_account)
        response = await self._provider_call(
            self._stripe_client.v1.payouts.cancel_async,
            payout_id,
            None,
            options,
        )
        return self._normalize_payout(self._to_raw(response))

    async def list_payouts(
        self,
        *,
        limit: int = 25,
        starting_after: str | None = None,
        stripe_account: str | None = None,
    ) -> StripeListResult:
        """List Stripe payouts."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()
        return await self._list_resource(
            self._stripe_client.v1.payouts.list_async,
            limit=limit,
            starting_after=starting_after,
            options=self._connect_options(stripe_account),
        )

    # ------------------------------------------------------------------
    # Balance
    # ------------------------------------------------------------------

    async def retrieve_balance(self, *, stripe_account: str | None = None) -> Balance:
        """Retrieve Stripe account balance."""
        if self._stripe_client is None:
            raise PaymentsNotConnectedError()

        options = self._connect_options(stripe_account)
        response = await self._provider_call(
            self._stripe_client.v1.balance.retrieve_async,
            None,
            options,
        )
        return self._normalize_balance(self._to_raw(response))

    # ------------------------------------------------------------------
    # Webhooks
    # ------------------------------------------------------------------

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
