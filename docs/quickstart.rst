Quickstart
==========

Define a table
--------------

.. code-block:: python

   # app/models.py
   import uuid
   from datetime import datetime

   from derp.orm import (
       Table, Field, UUID, Varchar, Text, Integer,
       Boolean, Timestamp,
   )

   class Product(Table, table="products"):
       id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
       name: str = Field(Varchar(255))
       description: str | None = Field(Text(), nullable=True)
       price_cents: int = Field(Integer())
       is_active: bool = Field(Boolean(), default="true")
       created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")

Configure
---------

.. code-block:: toml

   # derp.toml
   [database]
   db_url = "$DATABASE_URL"
   schema_path = "app/models.py"

Generate and apply a migration
------------------------------

.. code-block:: bash

   $ derp generate --name initial
   Found 1 table(s): Product
   Migration generated: migrations/0000_migration/

   $ derp migrate
   Applying 0000_migration... done

Query data
----------

.. code-block:: python

   from derp import DerpClient, DerpConfig
   from app.models import Product

   config = DerpConfig.load("derp.toml")
   derp = DerpClient(config)
   await derp.connect()

   # Insert
   product = await (
       derp.db.insert(Product)
       .values(name="Wireless Headphones", price_cents=4999)
       .returning(Product)
       .execute()
   )

   # Select
   active = await (
       derp.db.select(Product)
       .where(Product.c.is_active == True)
       .order_by(Product.c.created_at, asc=False)
       .limit(10)
       .execute()
   )

   # Update
   await (
       derp.db.update(Product)
       .set(price_cents=3999)
       .where(Product.c.id == product.id)
       .execute()
   )

   # Delete
   await (
       derp.db.delete(Product)
       .where(Product.c.id == product.id)
       .execute()
   )

Use in FastAPI
--------------

.. code-block:: python

   from contextlib import asynccontextmanager
   from collections.abc import AsyncIterator

   from fastapi import FastAPI, Request, Depends

   from derp import DerpClient, DerpConfig
   from app.models import Product

   @asynccontextmanager
   async def lifespan(app: FastAPI) -> AsyncIterator[None]:
       config = DerpConfig.load("derp.toml")
       derp = DerpClient(config)
       await derp.connect()
       app.state.derp = derp
       yield
       await derp.disconnect()

   app = FastAPI(lifespan=lifespan)

   def get_derp(request: Request) -> DerpClient:
       return request.app.state.derp

   @app.get("/products")
   async def list_products(derp: DerpClient = Depends(get_derp)):
       return await (
           derp.db.select(Product)
           .where(Product.c.is_active == True)
           .execute()
       )

Next steps: :doc:`guide/client` for the full lifecycle, :doc:`guide/orm` for all query features.
