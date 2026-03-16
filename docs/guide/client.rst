Client
======

App Lifecycle
-------------

Connect on startup, store on ``app.state``, disconnect on shutdown:

.. code-block:: python

   from contextlib import asynccontextmanager
   from collections.abc import AsyncIterator

   from fastapi import FastAPI

   from derp import DerpClient
   from derp.config import DerpConfig

   @asynccontextmanager
   async def lifespan(app: FastAPI) -> AsyncIterator[None]:
       config = DerpConfig.load("derp.toml")
       derp = DerpClient(config)
       await derp.connect()
       app.state.derp_client = derp
       yield
       await derp.disconnect()

   app = FastAPI(lifespan=lifespan)

FastAPI Dependency
------------------

Retrieve the client from ``app.state`` inside route handlers:

.. code-block:: python

   from fastapi import Request, Depends

   from derp import DerpClient

   def get_derp(request: Request) -> DerpClient:
       return request.app.state.derp_client

   @app.get("/products")
   async def list_products(derp: DerpClient = Depends(get_derp)):
       return await derp.db.select(Product).execute()

Accessing Services
------------------

All services are available as properties on the client:

.. code-block:: python

   derp.db         # DatabaseEngine  -- ORM queries
   derp.auth       # BaseAuthClient  -- sign up, sign in, sessions
   derp.kv         # KVClient        -- key-value store (Valkey)
   derp.storage    # StorageClient   -- file storage (S3)
   derp.payments   # PaymentsClient  -- payments (Stripe)
   derp.queue      # QueueClient     -- task queue (Celery / Vercel)

Each property raises ``ValueError`` if the corresponding section is
missing from ``derp.toml``.

Loading Config
--------------

.. code-block:: python

   from derp.config import DerpConfig

   # From file (resolves $ENV_VAR references)
   config = DerpConfig.load("derp.toml")

   # Access the raw config
   config.database.db_url
   config.auth.native.jwt.secret
