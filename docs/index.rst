Derp
====

An async Python backend toolkit. One client, one config file.

**ORM** · **Auth** · **Payments** · **Storage** · **KV** · **Queues** · **CLI** · **Studio**

.. code-block:: bash

   uv add derp-py

.. code-block:: python

   from derp import DerpClient, DerpConfig

   config = DerpConfig.load("derp.toml")
   derp = DerpClient(config)
   await derp.connect()

   products = await (
       derp.db.select(Product)
       .where(Product.c.is_active == True)
       .order_by(Product.c.created_at, asc=False)
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

      Valkey: caching, stampede protection, idempotency, webhook dedup.

   .. grid-item-card:: Task Queue
      :link: guide/queue
      :link-type: doc

      Celery or Vercel: enqueue, delay, schedules, status polling.

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
