Installation
============

Requires Python 3.12+.

.. tab-set::

   .. tab-item:: uv

      .. code-block:: bash

         uv add derp-py

   .. tab-item:: pip

      .. code-block:: bash

         pip install derp-py

Verify:

.. code-block:: bash

   $ derp version
   derp version 0.1.0

Initialize a project
--------------------

.. code-block:: bash

   $ derp init
   Created derp.toml

This creates a ``derp.toml`` with defaults. Set your database URL:

.. code-block:: bash

   export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb

Then define your schema, generate a migration, and apply it:

.. code-block:: bash

   $ derp generate --name initial
   $ derp migrate

See :doc:`quickstart` for a full walkthrough, or :doc:`guide/config` for all configuration options.

What's included
---------------

Derp installs with all modules — use only what you need by configuring the relevant sections in ``derp.toml``:

.. list-table::
   :widths: 20 35 25
   :header-rows: 1

   * - Module
     - What it does
     - Config section
   * - **ORM**
     - Typed PostgreSQL query builder, migrations
     - ``[database]``
   * - **Auth**
     - Email/password, magic links, OAuth, JWTs, orgs
     - ``[auth.native]`` or ``[auth.clerk]``
   * - **KV**
     - Valkey cache, stampede protection, idempotency
     - ``[kv.valkey]``
   * - **Storage**
     - S3-compatible file uploads/downloads
     - ``[storage]``
   * - **Payments**
     - Stripe: checkout, webhooks, Connect
     - ``[payments]``
   * - **Queue**
     - Celery or Vercel task queues, schedules
     - ``[queue.celery]`` or ``[queue.vercel]``
   * - **CLI**
     - ``derp init``, ``generate``, ``migrate``, ``push``, ``studio``
     - reads ``derp.toml``

Unconfigured modules are simply not initialized — no extra dependencies needed.
