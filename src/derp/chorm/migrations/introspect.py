"""Live-server introspection — build a :class:`SchemaSnapshot` from
``system.tables`` and ``system.columns``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from derp.chorm.migrations.snapshot import (
    ColumnSnapshot,
    EngineSnapshot,
    IndexSnapshot,
    ProjectionSnapshot,
    SchemaSnapshot,
    TableSnapshot,
)

if TYPE_CHECKING:
    from derp.chorm.engine import ClickHouseEngine


class _ParsedEngineFull(TypedDict):
    name: str | None
    args: tuple[str, ...]
    order_by: str | None
    partition_by: str | None
    primary_key: str | None
    sample_by: str | None
    ttl: str | None
    settings: dict[str, str]


async def introspect(
    engine: ClickHouseEngine,
    *,
    database: str = "default",
    include: list[str] | None = None,
    exclude_journal: bool = True,
) -> SchemaSnapshot:
    """Pull every table in *database* into a snapshot.

    Set *include* to a list of table names to limit the snapshot.
    """
    where = f"database = '{database}' AND engine NOT LIKE '%View'"
    if exclude_journal:
        where += " AND name NOT LIKE '\\_derp\\_chorm%'"
    if include:
        names = ",".join(f"'{n}'" for n in include)
        where += f" AND name IN ({names})"

    # ORDER BY on every query: snapshot content must be deterministic across
    # runs so pull/diff output (and any hash over it) is stable.
    tables_rows = await engine.fetch(
        f"""
        SELECT name, engine, engine_full, partition_key,
               sorting_key, primary_key, sampling_key, comment
        FROM system.tables WHERE {where}
        ORDER BY name
        """
    )

    # NB: system.columns exposes no per-column TTL (it only lives in
    # create_table_query), so column TTL is not introspected here.
    cols_rows = await engine.fetch(
        f"""
        SELECT table, name, type, default_kind, default_expression,
               compression_codec, comment
        FROM system.columns
        WHERE database = '{database}'
        ORDER BY table, position
        """
    )

    idx_rows = await engine.fetch(
        f"""
        SELECT table, name, expr, type, granularity
        FROM system.data_skipping_indices
        WHERE database = '{database}'
        ORDER BY table, name
        """
    )

    projection_rows = await engine.fetch(
        f"""
        SELECT table, name, query
        FROM system.projections
        WHERE database = '{database}'
        ORDER BY table, name
        """
    )

    cols_by_table: dict[str, list[ColumnSnapshot]] = {}
    for row in cols_rows:
        tname = row["table"]
        cols_by_table.setdefault(tname, []).append(
            ColumnSnapshot(
                name=row["name"],
                type=row["type"],
                default=(
                    row["default_expression"] or None
                    if row.get("default_kind") in ("DEFAULT", "")
                    else None
                ),
                materialized=(
                    row["default_expression"]
                    if row.get("default_kind") == "MATERIALIZED"
                    else None
                ),
                alias=(
                    row["default_expression"]
                    if row.get("default_kind") == "ALIAS"
                    else None
                ),
                ephemeral=row.get("default_kind") == "EPHEMERAL",
                codec=row.get("compression_codec") or None,
                ttl=None,
                comment=row.get("comment") or None,
            )
        )

    idx_by_table: dict[str, list[IndexSnapshot]] = {}
    for row in idx_rows:
        idx_by_table.setdefault(row["table"], []).append(
            IndexSnapshot(
                name=row["name"],
                expression=row["expr"],
                type=row["type"],
                granularity=int(row["granularity"]),
            )
        )

    projections_by_table: dict[str, list[ProjectionSnapshot]] = {}
    for row in projection_rows:
        select, order_by = _parse_projection_query(row.get("query") or "")
        projections_by_table.setdefault(row["table"], []).append(
            ProjectionSnapshot(
                name=row["name"],
                select=select,
                order_by=order_by,
            )
        )

    tables: list[TableSnapshot] = []
    for row in tables_rows:
        name = row["name"]
        engine_full = row.get("engine_full") or ""
        parsed = _parse_engine_full(engine_full)
        eng = EngineSnapshot(
            name=parsed.get("name") or row["engine"],
            args=parsed["args"],
            order_by=row.get("sorting_key") or parsed.get("order_by") or None,
            partition_by=row.get("partition_key") or parsed.get("partition_by") or None,
            primary_key=row.get("primary_key") or parsed.get("primary_key") or None,
            sample_by=row.get("sampling_key") or parsed.get("sample_by") or None,
            ttl=parsed["ttl"],
            settings=parsed["settings"],
        )
        tables.append(
            TableSnapshot(
                name=name,
                database=database,
                columns=cols_by_table.get(name, []),
                indexes=idx_by_table.get(name, []),
                projections=projections_by_table.get(name, []),
                engine=eng,
                comment=row.get("comment") or None,
            )
        )

    return SchemaSnapshot(tables=tables)


def _parse_engine_full(engine_full: str) -> _ParsedEngineFull:
    if not engine_full:
        return _empty_parsed_engine_full()

    name = engine_full.split("(", 1)[0].split()[0]
    rest = engine_full[len(name) :].strip()
    args: tuple[str, ...] = ()
    if rest.startswith("("):
        body, rest = _consume_parenthesized(rest)
        args = tuple(_split_top_level(body))

    clauses = _extract_clauses(rest)
    settings: dict[str, str] = {}
    if "SETTINGS" in clauses:
        for part in _split_top_level(clauses["SETTINGS"]):
            if "=" in part:
                k, v = part.split("=", 1)
                settings[k.strip()] = v.strip()

    return {
        "name": name,
        "args": args,
        "order_by": clauses.get("ORDER BY"),
        "partition_by": clauses.get("PARTITION BY"),
        "primary_key": clauses.get("PRIMARY KEY"),
        "sample_by": clauses.get("SAMPLE BY"),
        "ttl": clauses.get("TTL"),
        "settings": settings,
    }


def _empty_parsed_engine_full() -> _ParsedEngineFull:
    return {
        "name": None,
        "args": (),
        "order_by": None,
        "partition_by": None,
        "primary_key": None,
        "sample_by": None,
        "ttl": None,
        "settings": {},
    }


def _parse_projection_query(query: str) -> tuple[str, str | None]:
    query = query.strip()
    if query.upper().startswith("SELECT "):
        query = query[7:].strip()
    clauses = _extract_clauses(query)
    if "ORDER BY" not in clauses:
        return query, None
    marker = _find_keyword(query, "ORDER BY")
    if marker is None:
        return query, None
    return query[:marker].strip(), clauses["ORDER BY"]


def _consume_parenthesized(text: str) -> tuple[str, str]:
    depth = 0
    quote: str | None = None
    escaped = False
    chars: list[str] = []
    for i, ch in enumerate(text):
        if i == 0:
            depth = 1
            continue
        if quote:
            chars.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            continue
        if ch in ("'", '"', "`"):
            quote = ch
            chars.append(ch)
        elif ch == "(":
            depth += 1
            chars.append(ch)
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return "".join(chars).strip(), text[i + 1 :].strip()
            chars.append(ch)
        else:
            chars.append(ch)
    return "".join(chars).strip(), ""


def _split_top_level(text: str) -> list[str]:
    out: list[str] = []
    buf: list[str] = []
    depth = 0
    quote: str | None = None
    escaped = False
    for ch in text:
        if quote:
            buf.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            continue
        if ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            out.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        out.append("".join(buf).strip())
    return [p for p in out if p]


def _extract_clauses(text: str) -> dict[str, str]:
    keywords = (
        "PARTITION BY",
        "PRIMARY KEY",
        "ORDER BY",
        "SAMPLE BY",
        "TTL",
        "SETTINGS",
    )
    found = [
        (kw, pos) for kw in keywords if (pos := _find_keyword(text, kw)) is not None
    ]
    found.sort(key=lambda item: item[1])

    clauses: dict[str, str] = {}
    for i, (kw, pos) in enumerate(found):
        start = pos + len(kw)
        end = found[i + 1][1] if i + 1 < len(found) else len(text)
        clauses[kw] = text[start:end].strip()
    return clauses


def _find_keyword(text: str, keyword: str) -> int | None:
    upper = text.upper()
    target = keyword.upper()
    depth = 0
    quote: str | None = None
    escaped = False
    for i, ch in enumerate(text):
        if quote:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            continue
        if ch in ("'", '"', "`"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and upper.startswith(target, i):
            before = upper[i - 1] if i > 0 else " "
            after_pos = i + len(target)
            after = upper[after_pos] if after_pos < len(upper) else " "
            if not (before.isalnum() or before == "_") and not (
                after.isalnum() or after == "_"
            ):
                return i
    return None
