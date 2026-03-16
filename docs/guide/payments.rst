Payments
========

Async Stripe wrapper. Access it via ``derp.payments``.

Config
------

.. code-block:: toml

   # derp.toml
   [payments]
   api_key = "$STRIPE_SECRET_KEY"
   webhook_secret = "$STRIPE_WEBHOOK_SECRET"
   # max_network_retries = 2
   # timeout_seconds = 30.0

Create Customer
---------------

.. code-block:: python

   customer = await derp.payments.create_customer(
       email="buyer@example.com", name="Alice",
   )
   print(customer.id)  # "cus_xxx"

Retrieve and update:

.. code-block:: python

   customer = await derp.payments.retrieve_customer("cus_xxx")
   customer = await derp.payments.update_customer("cus_xxx", name="Alice Smith")

Checkout Session
----------------

.. code-block:: python

   session = await derp.payments.create_checkout_session(
       mode="payment",
       line_items=[{"price_id": "price_xxx", "quantity": 1}],
       success_url="https://example.com/success",
       cancel_url="https://example.com/cancel",
       customer_id=customer.id,
   )
   # Redirect user to session.url

For subscriptions, set ``mode="subscription"``.

Retrieve or expire a session:

.. code-block:: python

   session = await derp.payments.retrieve_checkout_session(session.id)
   session = await derp.payments.expire_checkout_session(session.id)

Webhook Verification
--------------------

.. code-block:: python

   event = await derp.payments.verify_webhook_event(
       payload=body, signature=sig_header,
   )
   if event.type == "checkout.session.completed":
       session_data = event.data_object
       ...

``verify_webhook_event`` uses the ``webhook_secret`` from config. Override per
call with the ``webhook_secret`` keyword argument.

Payment Intent
--------------

.. code-block:: python

   intent = await derp.payments.create_payment_intent(
       amount=2000,  # $20.00
       currency="usd",
       customer_id="cus_xxx",
   )

Confirm, capture, and cancel:

.. code-block:: python

   intent = await derp.payments.confirm_payment_intent(intent.id)
   intent = await derp.payments.capture_payment_intent(intent.id)
   intent = await derp.payments.cancel_payment_intent(
       intent.id, cancellation_reason="requested_by_customer",
   )

For manual capture (hold then capture), set ``capture_method="manual"``:

.. code-block:: python

   intent = await derp.payments.create_payment_intent(
       amount=5000,
       currency="usd",
       capture_method="manual",
   )

Refund
------

.. code-block:: python

   refund = await derp.payments.create_refund(payment_intent_id=intent.id)

Partial refund:

.. code-block:: python

   refund = await derp.payments.create_refund(
       payment_intent_id=intent.id, amount=500,
   )

Connect Accounts (Marketplace)
------------------------------

Create a connected account for a seller:

.. code-block:: python

   account = await derp.payments.create_account(
       type="express", country="US", email="seller@example.com",
   )

Generate an onboarding link:

.. code-block:: python

   link = await derp.payments.create_account_link(
       account_id=account.id,
       refresh_url="https://example.com/reauth",
       return_url="https://example.com/dashboard",
       type="account_onboarding",
   )
   # Redirect seller to link.url

Transfer funds to a connected account:

.. code-block:: python

   transfer = await derp.payments.create_transfer(
       amount=1500,
       currency="usd",
       destination=account.id,
   )

Split payments using ``transfer_data`` on a payment intent:

.. code-block:: python

   intent = await derp.payments.create_payment_intent(
       amount=3000,
       currency="usd",
       transfer_data={"destination": account.id},
       application_fee_amount=300,
   )

Payouts
-------

.. code-block:: python

   payout = await derp.payments.create_payout(
       amount=10000, currency="usd",
   )

Payout on behalf of a connected account:

.. code-block:: python

   payout = await derp.payments.create_payout(
       amount=5000, currency="usd", stripe_account=account.id,
   )

Balance
-------

.. code-block:: python

   balance = await derp.payments.retrieve_balance()
   # balance.available -> [{"amount": 10000, "currency": "usd"}]
   # balance.pending   -> [{"amount": 2000, "currency": "usd"}]
