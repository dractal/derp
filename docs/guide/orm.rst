ORM
===

Defining Tables
---------------

.. code-block:: python

   from derp.orm import (
       Table, Field, Fn, FK, Nullable, UUID, Varchar, Text,
       Integer, Boolean, TimestampTZ,
   )

   class Product(Table, table="products"):
       id: UUID = Field(primary=True, default=Fn.GEN_RANDOM_UUID)
       name: Varchar[255] = Field()
       description: Nullable[Text] = Field()
       price: Integer = Field()
       is_active: Boolean = Field(default=True)
       seller_id: UUID = Field(foreign_key=User.id, on_delete=FK.CASCADE)
       created_at: TimestampTZ = Field(default=Fn.NOW)

The column type is the **annotation** (e.g. ``Varchar[255]``), not a ``Field()``
argument. Use ``Nullable[Type]`` for nullable columns. Access columns via
``Product.name``, ``Product.price``, etc.

Field Types
-----------

.. list-table::
   :header-rows: 1
   :widths: 20 20

   * - Annotation
     - SQL Type
   * - ``UUID``
     - UUID
   * - ``Varchar[255]``
     - VARCHAR(255)
   * - ``Text``
     - TEXT
   * - ``Integer``
     - INTEGER
   * - ``BigInt``
     - BIGINT
   * - ``SmallInt``
     - SMALLINT
   * - ``Serial``
     - SERIAL
   * - ``Boolean``
     - BOOLEAN
   * - ``TimestampTZ``
     - TIMESTAMPTZ
   * - ``Timestamp``
     - TIMESTAMP
   * - ``Date``
     - DATE
   * - ``JSON``
     - JSON
   * - ``JSONB``
     - JSONB
   * - ``Numeric``
     - NUMERIC
   * - ``Enum[MyEnum]``
     - (derived from class name)
   * - ``Nullable[Type]``
     - (adds NULL)

Foreign Keys
------------

.. code-block:: python

   from derp.orm import FK

   seller_id: UUID = Field(foreign_key=User.id, on_delete=FK.CASCADE)

``FK`` members: ``CASCADE``, ``SET_NULL``, ``SET_DEFAULT``, ``RESTRICT``.
Lowercase strings (``"cascade"``, etc.) are also accepted.

SQL Functions (Fn)
------------------

Use the ``Fn`` enum for predefined SQL defaults instead of magic strings:

.. code-block:: python

   from derp.orm import Fn

   id: UUID = Field(primary=True, default=Fn.GEN_RANDOM_UUID)
   created_at: TimestampTZ = Field(default=Fn.NOW)

Members: ``GEN_RANDOM_UUID``, ``NOW``, ``CURRENT_TIMESTAMP``.

``Fn.to_tsvector(config, *columns)`` builds a tsvector expression:

.. code-block:: python

   Fn.to_tsvector("english", "title", "body")
   # → to_tsvector('english', title || ' ' || body)

Generated Columns
-----------------

Use ``generated`` to create columns computed by PostgreSQL on every write.
``generated`` and ``default`` are mutually exclusive.

.. code-block:: python

   class OrderLine(Table, table="order_lines"):
       price: Integer = Field()
       quantity: Integer = Field()
       amount: Integer = Field(generated="price * quantity")

This produces ``amount INTEGER GENERATED ALWAYS AS (price * quantity) STORED``.

A common use case is full-text search vectors:

.. code-block:: python

   from derp.orm import Fn

   class Article(Table, table="articles"):
       title: Varchar[255] = Field()
       body: Text = Field()
       search_vector: TSVector = Field(
           generated=Fn.to_tsvector("english", "title", "body")
       )

Select
------

.. code-block:: python

   db = derp.db

   # Basic select
   products = await db.select(Product).execute()

   # Filtering
   product = await (
       db.select(Product)
       .where(Product.id == product_id)
       .first_or_none()
   )

   # Operators
   results = await (
       db.select(Product)
       .where(Product.price > 1000)
       .where(Product.name.ilike("%phone%"))
       .where(Product.seller_id.in_([id1, id2]))
       .order_by(Product.price, asc=False)
       .limit(20)
       .offset(40)
       .execute()
   )

Available filter methods on columns: ``==``, ``!=``, ``>``, ``>=``,
``<``, ``<=``, ``.in_()``, ``.not_in()``, ``.like()``, ``.ilike()``,
``.is_null()``, ``.is_not_null()``, ``.between()``.

Joins
-----

.. code-block:: python

   orders = await (
       db.select(Order, User.email)
       .from_(Order)
       .inner_join(User, Order.user_id == User.id)
       .where(Order.total > 5000)
       .execute()
   )

Join methods: ``inner_join``, ``left_join``, ``right_join``,
``full_join``, ``cross_join``.

Insert
------

.. code-block:: python

   product = await (
       db.insert(Product)
       .values(name="Laptop", price=99900, seller_id=user_id)
       .returning(Product)
       .execute()
   )

   # Bulk insert (no returning)
   await (
       db.insert(Product)
       .values_list([
           {"name": "Mouse", "price": 2500, "seller_id": user_id},
           {"name": "Keyboard", "price": 7500, "seller_id": user_id},
       ])
       .execute()
   )

Update
------

.. code-block:: python

   updated = await (
       db.update(Product)
       .set(price=89900, is_active=False)
       .where(Product.id == product_id)
       .returning(Product)
       .execute()
   )

Delete
------

.. code-block:: python

   await (
       db.delete(Product)
       .where(Product.id == product_id)
       .execute()
   )

Transactions
------------

.. code-block:: python

   async with db.transaction() as txn:
       order = await (
           txn.insert(Order)
           .values(user_id=user_id, total=9900)
           .returning(Order)
           .execute()
       )
       await (
           txn.insert(OrderItem)
           .values(order_id=order.id, product_id=pid, qty=1)
           .execute()
       )

Rolls back automatically on exception, commits on clean exit.

Raw SQL
-------

.. code-block:: python

   from derp.orm import sql

   results = await (
       db.select(Product.name, sql("UPPER(name)").as_("upper_name"))
       .from_(Product)
       .execute()
   )

   # With parameters
   rows = await db.execute(
       "SELECT * FROM products WHERE price > $1", [5000]
   )

Aggregates
----------

.. code-block:: python

   stats = await (
       db.select(
           Product.price.sum().as_("total"),
           Product.price.avg().as_("average"),
           Product.id.count().as_("count"),
           Product.price.min().as_("cheapest"),
           Product.price.max().as_("priciest"),
       )
       .from_(Product)
       .execute()
   )

Group By / Having
-----------------

.. code-block:: python

   sales = await (
       db.select(Product.seller_id, Product.price.sum().as_("revenue"))
       .from_(Product)
       .group_by(Product.seller_id)
       .having(Product.price.sum() > 100000)
       .execute()
   )

Subqueries and EXISTS
---------------------

.. code-block:: python

   # Subquery in WHERE
   active_sellers = db.select(User.id).where(User.is_active)

   products = await (
       db.select(Product)
       .where(Product.seller_id.in_(active_sellers))
       .execute()
   )

   # EXISTS
   has_orders = db.select(Order).where(Order.user_id == User.id)

   users = await (
       db.select(User)
       .where(has_orders.exists())
       .execute()
   )

Set Operations
--------------

.. code-block:: python

   recent = db.select(Product).where(Product.created_at > cutoff)
   popular = db.select(Product).where(Product.sales > 100)

   combined = await recent.union(popular).execute()
   overlap  = await recent.intersect(popular).execute()
   only_new = await recent.except_(popular).execute()
