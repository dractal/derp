KV
==

Async key-value client backed by Valkey (Redis-compatible). Access it via ``derp.kv``.

Config
------

.. code-block:: toml

   # derp.toml
   [kv.valkey]
   addresses = [["localhost", 6379]]
   # username = "$VALKEY_USERNAME"
   # password = "$VALKEY_PASSWORD"
   # use_tls = false
   # mode = "standalone"  # or "cluster"

Basic Operations
----------------

.. code-block:: python

   # Set a key with a 1-hour TTL
   await derp.kv.set(b"user:123", b'{"name":"Alice"}', ttl=3600)

   # Get a key
   data = await derp.kv.get(b"user:123")

   # Delete a key
   await derp.kv.delete(b"user:123")

   # Check existence
   if await derp.kv.exists(b"user:123"):
       ...

All keys and values are ``bytes``. TTL is in seconds.

Batch Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Set multiple keys
   await derp.kv.mset([
       (b"user:1", b'{"name":"Alice"}'),
       (b"user:2", b'{"name":"Bob"}'),
   ], ttl=3600)

   # Get multiple keys
   values = await derp.kv.mget([b"user:1", b"user:2"])

   # Delete multiple keys
   deleted_count = await derp.kv.delete_many([b"user:1", b"user:2"])

Other Primitives
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Set only if key does not exist (returns True if set)
   was_set = await derp.kv.set_nx(b"lock:order:99", b"1", ttl=10)

   # Check remaining TTL
   remaining = await derp.kv.ttl(b"user:123")

   # Update TTL on existing key
   await derp.kv.expire(b"user:123", ttl=7200)

   # Scan keys by prefix
   async for key in derp.kv.scan(prefix=b"user:"):
       print(key)

Cache with Stampede Protection
------------------------------

``guarded_get`` prevents multiple concurrent callers from recomputing the same
expensive value on a cache miss. One caller acquires a lock and computes; the
rest wait for the cache to be populated.

.. code-block:: python

   result = await derp.kv.guarded_get(
       b"product:42",
       compute=lambda: fetch_from_db(42),
       ttl=300,
   )

``compute`` is an async callable returning ``bytes``. If the lock holder is
slow, waiters retry for up to ``lock_ttl`` seconds (default 2s) before falling
through and computing directly.

.. code-block:: python

   result = await derp.kv.guarded_get(
       b"product:42",
       compute=lambda: fetch_from_db(42),
       ttl=300,
       lock_ttl=5.0,
       retry_delay=0.1,
   )

Idempotent API Endpoints
------------------------

``idempotent_execute`` runs a computation once per idempotency key and caches
the result (body + status code). Subsequent calls with the same key return the
cached response. Uses ``guarded_get`` internally for stampede protection.

.. code-block:: python

   idem_key = request.headers.get("Idempotency-Key")
   if idem_key:
       body, status, is_replay = await derp.kv.idempotent_execute(
           key=idem_key,
           compute=lambda: create_order(data),
           status_code=201,
       )
       return JSONResponse(body, status_code=status)

Returns a tuple of ``(body, status_code, is_replay)``. ``is_replay`` is
``True`` when the cached value was returned. Default TTL is 24 hours.

.. code-block:: python

   body, status, is_replay = await derp.kv.idempotent_execute(
       key=idem_key,
       compute=lambda: create_order(data),
       status_code=201,
       ttl=3600,               # cache for 1 hour instead of 24h
       key_prefix="myapp:idem", # custom key prefix
   )

Webhook Deduplication
---------------------

``already_processed`` atomically marks an event ID and returns whether it was
already seen. Useful for making webhook handlers idempotent.

.. code-block:: python

   if await derp.kv.already_processed(event_id=event["id"]):
       return {"status": "duplicate"}

   # First time seeing this event -- process it
   handle_event(event)

Default TTL is 24 hours. Customize with ``ttl`` and ``key_prefix``:

.. code-block:: python

   if await derp.kv.already_processed(
       event_id=event["id"],
       ttl=7200,
       key_prefix="myapp:webhook",
   ):
       return {"status": "duplicate"}
