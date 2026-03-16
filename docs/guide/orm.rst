ORM
===

Defining Tables
---------------

.. code-block:: python

   import uuid
   from datetime import datetime

   from derp.orm import (
       Table, Field, UUID, Varchar, Text, Integer, Boolean,
       Timestamp, ForeignKey, ForeignKeyAction,
   )

   class Product(Table, table="products"):
       id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
       name: str = Field(Varchar(255))
       description: str | None = Field(Text(), nullable=True)
       price: int = Field(Integer())
       is_active: bool = Field(Boolean(), default=True)
       seller_id: uuid.UUID = Field(
           UUID(),
           foreign_key=ForeignKey("users.id", on_delete=ForeignKeyAction.CASCADE),
           index=True,
       )
       created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")

Access columns via ``Product.c.name``, ``Product.c.price``, etc.

Field Types
-----------

.. list-table::
   :header-rows: 1
   :widths: 20 20 20

   * - Type Class
     - SQL Type
     - Python Type
   * - ``UUID()``
     - UUID
     - ``uuid.UUID``
   * - ``Varchar(n)``
     - VARCHAR(n)
     - ``str``
   * - ``Text()``
     - TEXT
     - ``str``
   * - ``Integer()``
     - INTEGER
     - ``int``
   * - ``BigInt()``
     - BIGINT
     - ``int``
   * - ``SmallInt()``
     - SMALLINT
     - ``int``
   * - ``Serial()``
     - SERIAL
     - ``int``
   * - ``Boolean()``
     - BOOLEAN
     - ``bool``
   * - ``Timestamp()``
     - TIMESTAMP
     - ``datetime``
   * - ``Timestamp(with_timezone=True)``
     - TIMESTAMPTZ
     - ``datetime``
   * - ``Date()``
     - DATE
     - ``date``
   * - ``JSON()``
     - JSON
     - ``Any``
   * - ``JSONB()``
     - JSONB
     - ``Any``
   * - ``Array(Integer())``
     - INTEGER[]
     - ``list[int]``
   * - ``Enum(Status)``
     - status
     - ``Status``
   * - ``Numeric(10, 2)``
     - NUMERIC(10,2)
     - ``Decimal``

Foreign Keys
------------

.. code-block:: python

   from derp.orm import Field, UUID, ForeignKey, ForeignKeyAction

   seller_id: uuid.UUID = Field(
       UUID(),
       foreign_key=ForeignKey("users.id", on_delete=ForeignKeyAction.CASCADE),
   )

Actions: ``CASCADE``, ``SET_NULL``, ``SET_DEFAULT``, ``RESTRICT``, ``NO_ACTION``.

Select
------

.. code-block:: python

   db = derp.db

   # Basic select
   products = await db.select(Product).execute()

   # Filtering
   product = await (
       db.select(Product)
       .where(Product.c.id == product_id)
       .first_or_none()
   )

   # Operators
   results = await (
       db.select(Product)
       .where(Product.c.price > 1000)
       .where(Product.c.name.ilike("%phone%"))
       .where(Product.c.seller_id.in_([id1, id2]))
       .order_by(Product.c.price, asc=False)
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
       db.select(Order, User.c.email)
       .from_(Order)
       .inner_join(User, Order.c.user_id == User.c.id)
       .where(Order.c.total > 5000)
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
       .where(Product.c.id == product_id)
       .returning(Product)
       .execute()
   )

Delete
------

.. code-block:: python

   await (
       db.delete(Product)
       .where(Product.c.id == product_id)
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
       db.select(Product.c.name, sql("UPPER(name)").as_("upper_name"))
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
           Product.c.price.sum().as_("total"),
           Product.c.price.avg().as_("average"),
           Product.c.id.count().as_("count"),
           Product.c.price.min().as_("cheapest"),
           Product.c.price.max().as_("priciest"),
       )
       .from_(Product)
       .execute()
   )

Group By / Having
-----------------

.. code-block:: python

   sales = await (
       db.select(Product.c.seller_id, Product.c.price.sum().as_("revenue"))
       .from_(Product)
       .group_by(Product.c.seller_id)
       .having(Product.c.price.sum() > 100000)
       .execute()
   )

Subqueries and EXISTS
---------------------

.. code-block:: python

   # Subquery in WHERE
   active_sellers = db.select(User.c.id).where(User.c.is_active == True)

   products = await (
       db.select(Product)
       .where(Product.c.seller_id.in_(active_sellers))
       .execute()
   )

   # EXISTS
   has_orders = db.select(Order).where(Order.c.user_id == User.c.id)

   users = await (
       db.select(User)
       .where(has_orders.exists())
       .execute()
   )

Set Operations
--------------

.. code-block:: python

   recent = db.select(Product).where(Product.c.created_at > cutoff)
   popular = db.select(Product).where(Product.c.sales > 100)

   combined = await recent.union(popular).execute()
   overlap  = await recent.intersect(popular).execute()
   only_new = await recent.except_(popular).execute()
