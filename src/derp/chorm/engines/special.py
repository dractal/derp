"""Special-purpose ClickHouse table engines."""

from __future__ import annotations

from typing import Any

from derp.chorm.engines.base import TableEngine, _render_arg, quoted


class _ParamEngine(TableEngine):
    """Engine that takes positional args."""

    _name: str = ""
    _args: tuple[Any, ...] = ()

    def engine_clause(self) -> str:
        if not self._args:
            return f"ENGINE = {self._name}()"
        return f"ENGINE = {self._name}({', '.join(_render_arg(a) for a in self._args)})"


class Memory(_ParamEngine):
    """``Memory`` — in-RAM table."""

    _name = "Memory"
    _args = ()


class Null(_ParamEngine):
    """``Null`` — discards inserts."""

    _name = "Null"
    _args = ()


class Buffer(_ParamEngine):
    """``Buffer(...)`` — buffers inserts in RAM, flushing to a target table.

    Args mirror the ClickHouse signature:
    ``Buffer(database, table, num_layers, min_time, max_time,
    min_rows, max_rows, min_bytes, max_bytes)``.
    """

    _name = "Buffer"

    def __init__(
        self,
        database: str,
        table: str,
        num_layers: int = 16,
        min_time: int = 10,
        max_time: int = 100,
        min_rows: int = 10_000,
        max_rows: int = 1_000_000,
        min_bytes: int = 10_000_000,
        max_bytes: int = 100_000_000,
    ) -> None:
        self._args = (
            database,
            table,
            num_layers,
            min_time,
            max_time,
            min_rows,
            max_rows,
            min_bytes,
            max_bytes,
        )


class Distributed(_ParamEngine):
    """``Distributed(cluster, database, table[, sharding_key[, policy_name]])``."""

    _name = "Distributed"

    def __init__(
        self,
        cluster: str,
        database: str,
        table: str,
        sharding_key: str | None = None,
        policy_name: str | None = None,
    ) -> None:
        args: list[Any] = [cluster, database, table]
        if sharding_key is not None:
            args.append(sharding_key)
        if policy_name is not None:
            args.append(quoted(policy_name))
        self._args = tuple(args)


class Merge(_ParamEngine):
    """``Merge(database, regex)`` — read-only union over matching tables."""

    _name = "Merge"

    def __init__(self, database: str, table_regex: str) -> None:
        self._args = (database, quoted(table_regex))


class Dictionary(_ParamEngine):
    """``Dictionary(dict_name)``."""

    _name = "Dictionary"

    def __init__(self, dict_name: str) -> None:
        self._args = (dict_name,)


class Join(_ParamEngine):
    """``Join(strictness, kind, key1[, key2 …])``."""

    _name = "Join"

    def __init__(self, strictness: str, kind: str, *keys: str) -> None:
        self._args = (strictness, kind, *keys)


class SetEngine(_ParamEngine):
    """``Set()`` — read-only data set for IN."""

    _name = "Set"


class URL(_ParamEngine):
    """``URL(uri, format)``."""

    _name = "URL"

    def __init__(self, uri: str, format: str) -> None:
        self._args = (quoted(uri), format)


class File(_ParamEngine):
    """``File(format[, path])``."""

    _name = "File"

    def __init__(self, format: str, path: str | None = None) -> None:
        if path is None:
            self._args = (format,)
        else:
            self._args = (format, quoted(path))


class View(_ParamEngine):
    """``View`` — schema-only marker; the actual SELECT goes into the DDL."""

    _name = "View"


class MaterializedView(_ParamEngine):
    """``MaterializedView`` — schema-only marker.

    The user supplies the source SELECT through the DDL builder.
    """

    _name = "MaterializedView"


class S3(_ParamEngine):
    """``S3(path[, access_key_id, secret_access_key,] format[, compression])``."""

    _name = "S3"

    def __init__(
        self,
        path: str,
        format: str,
        *,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        compression: str | None = None,
    ) -> None:
        args: list[Any] = [quoted(path)]
        if aws_access_key_id is not None and aws_secret_access_key is not None:
            args.append(quoted(aws_access_key_id))
            args.append(quoted(aws_secret_access_key))
        args.append(quoted(format))
        if compression is not None:
            args.append(quoted(compression))
        self._args = tuple(args)


class HDFS(_ParamEngine):
    """``HDFS(uri, format)``."""

    _name = "HDFS"

    def __init__(self, uri: str, format: str) -> None:
        self._args = (quoted(uri), quoted(format))


class MySQL(_ParamEngine):
    """``MySQL(host_port, db, table, user, password[, replace_query[, on_dup]])``."""

    _name = "MySQL"

    def __init__(
        self,
        host_port: str,
        database: str,
        table: str,
        user: str,
        password: str,
        *,
        replace_query: bool = False,
        on_duplicate_clause: str | None = None,
    ) -> None:
        args: list[Any] = [
            quoted(host_port),
            quoted(database),
            quoted(table),
            quoted(user),
            quoted(password),
        ]
        if on_duplicate_clause is not None or replace_query:
            args.append(1 if replace_query else 0)
        if on_duplicate_clause is not None:
            args.append(quoted(on_duplicate_clause))
        self._args = tuple(args)


class PostgreSQL(_ParamEngine):
    """``PostgreSQL(host_port, db, table, user, password[, schema[, on_conflict]])``."""

    _name = "PostgreSQL"

    def __init__(
        self,
        host_port: str,
        database: str,
        table: str,
        user: str,
        password: str,
        *,
        schema: str | None = None,
        on_conflict: str | None = None,
    ) -> None:
        args: list[Any] = [
            quoted(host_port),
            quoted(database),
            quoted(table),
            quoted(user),
            quoted(password),
        ]
        if schema is not None:
            args.append(quoted(schema))
        if on_conflict is not None:
            args.append(quoted(on_conflict))
        self._args = tuple(args)


class ODBC(_ParamEngine):
    """``ODBC(connection_settings, external_database, external_table)``."""

    _name = "ODBC"

    def __init__(
        self,
        connection_settings: str,
        external_database: str,
        external_table: str,
    ) -> None:
        self._args = (
            quoted(connection_settings),
            quoted(external_database),
            quoted(external_table),
        )


class JDBC(_ParamEngine):
    """``JDBC(datasource, external_database, external_table)``."""

    _name = "JDBC"

    def __init__(
        self,
        datasource: str,
        external_database: str,
        external_table: str,
    ) -> None:
        self._args = (
            quoted(datasource),
            quoted(external_database),
            quoted(external_table),
        )


class KafkaEngine(TableEngine):
    """``Kafka()`` engine with settings.

    Example::

        engine = KafkaEngine(
            broker_list="localhost:9092",
            topic_list="events",
            group_name="my_group",
            format="JSONEachRow",
        )
    """

    def __init__(
        self,
        *,
        broker_list: str,
        topic_list: str,
        group_name: str,
        format: str,
        row_delimiter: str | None = None,
        schema: str | None = None,
        num_consumers: int = 1,
        max_block_size: int | None = None,
    ) -> None:
        self._kw = {
            "kafka_broker_list": broker_list,
            "kafka_topic_list": topic_list,
            "kafka_group_name": group_name,
            "kafka_format": format,
        }
        if row_delimiter is not None:
            self._kw["kafka_row_delimiter"] = row_delimiter
        if schema is not None:
            self._kw["kafka_schema"] = schema
        if num_consumers != 1:
            self._kw["kafka_num_consumers"] = num_consumers
        if max_block_size is not None:
            self._kw["kafka_max_block_size"] = max_block_size

    def engine_clause(self) -> str:
        return "ENGINE = Kafka()"

    def settings_clause(self) -> str | None:
        parts = []
        for k, v in self._kw.items():
            if isinstance(v, int | float):
                parts.append(f"{k} = {v}")
            else:
                escaped = str(v).replace("'", "\\'")
                parts.append(f"{k} = '{escaped}'")
        return f"SETTINGS {', '.join(parts)}"
