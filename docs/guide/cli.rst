CLI
===

Command Reference
-----------------

.. list-table::
   :header-rows: 1
   :widths: 18 42 25

   * - Command
     - Description
     - Key Flags
   * - ``init``
     - Create ``derp.toml`` config file
     - ``--force``
   * - ``generate``
     - Generate migration from schema diff
     - ``--name``, ``--force``, ``--custom``
   * - ``migrate``
     - Apply pending migrations to database
     - ``--dry-run``
   * - ``push``
     - Push schema directly to database (dev)
     - ``--force``, ``--dry-run``
   * - ``pull``
     - Introspect database into snapshot
     - ``--out``, ``--migration``, ``--name``
   * - ``status``
     - Show applied vs pending migrations
     -
   * - ``check``
     - Verify schema matches latest snapshot
     -
   * - ``drop``
     - Remove migration files (not database)
     - ``--all``, ``--force``, ``VERSION``
   * - ``studio``
     - Launch web UI for browsing the database
     - ``--host``, ``--port``

init
----

.. code-block:: bash

   derp init

Creates a ``derp.toml`` with sensible defaults. Use ``--force`` to
overwrite an existing file.

generate
--------

.. code-block:: bash

   derp generate --name add_orders

Compares your Python schema against the latest snapshot, detects
changes (new tables, added/dropped columns, type changes), and writes
a migration folder with ``migration.sql`` and ``snapshot.json``.

Use ``--custom`` to create an empty migration for hand-written SQL.

migrate
-------

.. code-block:: bash

   derp migrate

Applies all pending migrations inside a transaction. Each migration is
recorded in ``derp_migrations`` with a SHA-256 hash.

``--dry-run`` prints the SQL without executing.

push
----

.. code-block:: bash

   derp push

Diffs your schema against the **live database** (via introspection) and
applies changes directly. Intended for development only -- use
``generate`` + ``migrate`` in production.

pull
----

.. code-block:: bash

   derp pull --migration --name baseline

Introspects the database and writes a snapshot. With ``--migration`` it
creates a journal entry so subsequent ``generate`` calls diff from this
baseline.

status
------

.. code-block:: bash

   derp status

Prints each migration version with ``[x]`` (applied) or ``[ ]``
(pending), plus the timestamp it was applied.

check
-----

.. code-block:: bash

   derp check

Exits ``0`` if schema matches the latest snapshot, ``1`` if changes are
detected. Useful in CI to enforce that migrations are committed.

drop
----

.. code-block:: bash

   derp drop 0003
   derp drop --all

Removes migration files and updates the journal. Does **not** reverse
any changes in the database.

studio
------

.. code-block:: bash

   derp studio --port 4983

Launches a web UI for browsing tables, columns, and relationships in
your database.

Typical Dev Workflow
--------------------

.. code-block:: bash

   # 1. Scaffold config
   derp init

   # 2. Write your schema in Python, then generate migration
   derp generate --name initial

   # 3. Review migration.sql, then apply
   derp migrate

   # Quick iteration (skips migration files)
   derp push
