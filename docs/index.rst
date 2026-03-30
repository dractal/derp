Derp
====

An async Python backend toolkit. One client, one config file.

**ORM** · **Auth** · **Payments** · **Storage** · **KV** · **Queues** · **AI** · **CLI** · **Studio**

.. warning::

   Derp is in alpha. The API is unstable and may change without notice before 1.0.

.. code-block:: bash

   uv add derp-py

.. code-block:: python

   from derp import DerpClient, DerpConfig

   config = DerpConfig.load("derp.toml")
   derp = DerpClient(config)
   await derp.connect()

   products = await (
       derp.db.select(Product)
       .where(Product.is_active)
       .order_by(Product.created_at, asc=False)
       .limit(10)
       .execute()
   )

.. grid:: 2
   :gutter: 3

   .. grid-item-card:: ORM & Query Builder
      :link: guide/orm
      :link-type: doc

      Typed tables, fluent queries, joins, aggregates, transactions, migrations.

   .. grid-item-card:: Auth
      :link: guide/auth
      :link-type: doc

      Email/password, magic links, OAuth, JWTs, organizations. Native or Clerk.

   .. grid-item-card:: Payments
      :link: guide/payments
      :link-type: doc

      Stripe: customers, checkout, webhooks, payment intents, Connect accounts.

   .. grid-item-card:: Storage
      :link: guide/storage
      :link-type: doc

      S3-compatible file uploads, downloads, metadata, bucket management.

   .. grid-item-card:: KV Store
      :link: guide/kv
      :link-type: doc

      Valkey: caching, stampede protection, idempotency, webhook dedup, rate limiting.

   .. grid-item-card:: Task Queue
      :link: guide/queue
      :link-type: doc

      Celery or Vercel: enqueue, delay, schedules, status polling.

   .. grid-item-card:: AI
      :link: guide/ai
      :link-type: doc

      OpenAI, Fal, Modal: chat, streaming with Vercel/TanStack adapters.

.. toctree::
   :maxdepth: 2
   :caption: Getting Started
   :hidden:

   installation
   quickstart

.. toctree::
   :maxdepth: 2
   :caption: Guides
   :hidden:

   guide/client
   guide/orm
   guide/auth
   guide/kv
   guide/storage
   guide/payments
   guide/queue
   guide/ai
   guide/cli
   guide/config

.. toctree::
   :maxdepth: 2
   :caption: Examples
   :hidden:

   examples/messaging

.. toctree::
   :maxdepth: 2
   :caption: API Reference
   :hidden:

   api/index
