Queue
=====

Async task queue supporting Celery and Vercel backends. Access it via
``derp.queue``.

Config -- Celery
----------------

.. code-block:: toml

   # derp.toml
   [queue.celery]
   broker_url = "$CELERY_BROKER_URL"
   result_backend = "$CELERY_RESULT_BACKEND"
   # task_serializer = "json"
   # result_serializer = "json"
   # task_default_queue = "default"

Config -- Vercel
----------------

.. code-block:: toml

   # derp.toml
   [queue.vercel]
   api_token = "$VERCEL_QUEUE_TOKEN"
   team_id = "team_xxx"
   project_id = "prj_xxx"
   # default_queue = "default"

Only one backend can be configured at a time.

Enqueue a Task
--------------

.. code-block:: python

   task_id = await derp.queue.enqueue(
       "send_welcome_email",
       payload={"user_id": str(user.id)},
   )

Route to a specific queue:

.. code-block:: python

   task_id = await derp.queue.enqueue(
       "generate_report",
       payload={"report_id": str(report.id)},
       queue="heavy",
   )

Delayed Task
------------

.. code-block:: python

   from datetime import timedelta

   task_id = await derp.queue.enqueue(
       "expire_reservation",
       payload={"reservation_id": str(res.id)},
       delay=timedelta(minutes=15),
   )

Check Status
------------

.. code-block:: python

   status = await derp.queue.get_status(task_id)
   print(status.state)   # "pending", "started", "success", "failure", ...
   print(status.result)  # available on success (Celery only)
   print(status.error)   # available on failure (Celery only)

Vercel queues do not expose per-message status; ``state`` will be ``"unknown"``.

Schedules
---------

Define recurring tasks in ``derp.toml``. Schedules are automatically registered
with the queue backend on connect.

Cron schedule:

.. code-block:: toml

   [[queue.schedules]]
   name = "cleanup-expired"
   task = "cleanup_expired_sessions"
   cron = "0 */6 * * *"

Interval schedule (Celery only):

.. code-block:: toml

   [[queue.schedules]]
   name = "sync-inventory"
   task = "sync_inventory"
   interval_seconds = 120

With payload and queue routing:

.. code-block:: toml

   [[queue.schedules]]
   name = "daily-digest"
   task = "send_daily_digest"
   cron = "0 9 * * *"
   payload = { timezone = "America/New_York" }
   queue = "email"

Each schedule must set exactly one of ``cron`` or ``interval_seconds``.

For Vercel, schedules translate to ``vercel.json`` cron entries. Set a custom
``path`` per schedule or it defaults to ``/api/cron/<name>``:

.. code-block:: toml

   [[queue.schedules]]
   name = "cleanup-expired"
   task = "cleanup_expired_sessions"
   cron = "0 */6 * * *"
   path = "/api/tasks/cleanup"

Running the Celery Worker
-------------------------

The Celery app is exposed at ``derp.queue.celery:app``:

.. code-block:: bash

   celery -A 'derp.queue.celery:app' worker --loglevel=info

To run the beat scheduler for recurring tasks:

.. code-block:: bash

   celery -A 'derp.queue.celery:app' beat --loglevel=info
