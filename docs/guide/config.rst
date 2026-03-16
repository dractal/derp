Config
======

Full Example
------------

.. code-block:: toml

   [database]
   db_url = "$DATABASE_URL"
   schema_path = "src/schema.py"
   migrations_dir = "./migrations"
   pool_min_size = 2
   pool_max_size = 5

   [email]
   site_name = "My App"
   site_url = "https://app.example.com"
   from_email = "noreply@example.com"
   smtp_host = "smtp.example.com"
   smtp_port = 587
   smtp_user = "$SMTP_USER"
   smtp_password = "$SMTP_PASSWORD"

   [storage]
   endpoint_url = "https://s3.amazonaws.com"
   access_key_id = "$AWS_ACCESS_KEY_ID"
   secret_access_key = "$AWS_SECRET_ACCESS_KEY"
   region = "us-east-1"

   [auth.native]
   enable_signup = true
   enable_confirmation = true
   enable_magic_link = false

   [auth.native.jwt]
   secret = "$JWT_SECRET"
   algorithm = "HS256"
   access_token_expire_minutes = 15
   refresh_token_expire_days = 7

   [auth.native.google_oauth]
   client_id = "$GOOGLE_CLIENT_ID"
   client_secret = "$GOOGLE_CLIENT_SECRET"
   redirect_uri = "https://app.example.com/auth/callback/google"

   [kv.valkey]
   addresses = [["localhost", 6379]]
   password = "$VALKEY_PASSWORD"

   [payments]
   api_key = "$STRIPE_SECRET_KEY"
   webhook_secret = "$STRIPE_WEBHOOK_SECRET"

   [queue.celery]
   broker_url = "$CELERY_BROKER_URL"
   result_backend = "$CELERY_RESULT_BACKEND"
   task_default_queue = "default"

   [[queue.schedules]]
   name = "daily-digest"
   task = "send_digest"
   cron = "0 9 * * *"

Environment Variables
---------------------

Any string value starting with ``$`` is resolved from the environment:

.. code-block:: toml

   db_url = "$DATABASE_URL"

Missing env vars raise ``ConfigError`` at load time.

Section Reference
-----------------

``[database]``
~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``db_url``
     - ``str``
     - PostgreSQL connection URL (required)
   * - ``replica_url``
     - ``str | None``
     - Read-replica URL
   * - ``schema_path``
     - ``str``
     - Path to schema module (required)
   * - ``migrations_dir``
     - ``str``
     - Default ``./migrations``
   * - ``pool_min_size``
     - ``int``
     - Default ``2``
   * - ``pool_max_size``
     - ``int``
     - Default ``5``
   * - ``statement_cache_size``
     - ``int``
     - Default ``0`` (PgBouncer-safe)

``[email]``
~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``site_name``
     - ``str``
     - Name shown in emails (required)
   * - ``site_url``
     - ``str``
     - Base URL for links (required)
   * - ``from_email``
     - ``str``
     - Sender address (required)
   * - ``smtp_host``
     - ``str``
     - SMTP server (required)
   * - ``smtp_port``
     - ``int``
     - SMTP port (required)
   * - ``smtp_user``
     - ``str``
     - SMTP username (required)
   * - ``smtp_password``
     - ``str``
     - SMTP password (required)

``[storage]``
~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``endpoint_url``
     - ``str | None``
     - S3-compatible endpoint
   * - ``access_key_id``
     - ``str | None``
     - AWS access key
   * - ``secret_access_key``
     - ``str | None``
     - AWS secret key
   * - ``region``
     - ``str``
     - Default ``auto``

``[auth.native]``
~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``enable_signup``
     - ``bool``
     - Default ``true``
   * - ``enable_confirmation``
     - ``bool``
     - Default ``true``
   * - ``enable_magic_link``
     - ``bool``
     - Default ``false``
   * - ``magic_link_expire_minutes``
     - ``int``
     - Default ``60``
   * - ``session_expire_days``
     - ``int``
     - Default ``30``

``[auth.native.jwt]``
~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``secret``
     - ``str``
     - Signing secret (required)
   * - ``algorithm``
     - ``str``
     - Default ``HS256``
   * - ``access_token_expire_minutes``
     - ``int``
     - Default ``15``
   * - ``refresh_token_expire_days``
     - ``int``
     - Default ``7``
   * - ``issuer``
     - ``str | None``
     - JWT ``iss`` claim
   * - ``audience``
     - ``str | None``
     - JWT ``aud`` claim

``[auth.clerk]``
~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``secret_key``
     - ``str``
     - Clerk secret key (required)
   * - ``jwt_key``
     - ``str | None``
     - Custom JWT verification key
   * - ``authorized_parties``
     - ``list[str]``
     - Allowed ``azp`` claims

``[kv.valkey]``
~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``addresses``
     - ``list[[str, int]]``
     - Default ``[["localhost", 6379]]``
   * - ``username``
     - ``str | None``
     - Valkey username
   * - ``password``
     - ``str | None``
     - Valkey password
   * - ``use_tls``
     - ``bool``
     - Default ``false``
   * - ``mode``
     - ``str``
     - ``standalone`` (default) or ``cluster``

``[payments]``
~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``api_key``
     - ``str``
     - Stripe secret key (required)
   * - ``webhook_secret``
     - ``str | None``
     - Stripe webhook secret
   * - ``max_network_retries``
     - ``int``
     - Default ``2``
   * - ``timeout_seconds``
     - ``float``
     - Default ``30.0``

``[queue.celery]``
~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``broker_url``
     - ``str``
     - Broker URL (required)
   * - ``result_backend``
     - ``str | None``
     - Result backend URL
   * - ``task_serializer``
     - ``str``
     - Default ``json``
   * - ``task_default_queue``
     - ``str``
     - Default ``default``

``[queue.vercel]``
~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``api_token``
     - ``str``
     - Vercel API token (required)
   * - ``team_id``
     - ``str | None``
     - Vercel team ID
   * - ``project_id``
     - ``str | None``
     - Vercel project ID
   * - ``default_queue``
     - ``str``
     - Default ``default``

``[[queue.schedules]]``
~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 15 30

   * - ``name``
     - ``str``
     - Schedule name (required)
   * - ``task``
     - ``str``
     - Task identifier (required)
   * - ``cron``
     - ``str | None``
     - Cron expression (mutually exclusive with ``interval_seconds``)
   * - ``interval_seconds``
     - ``float | None``
     - Interval in seconds
   * - ``payload``
     - ``dict | None``
     - JSON payload sent with each invocation
   * - ``queue``
     - ``str | None``
     - Target queue name
   * - ``path``
     - ``str | None``
     - Webhook path (Vercel)
