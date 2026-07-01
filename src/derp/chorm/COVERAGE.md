# ClickHouse ORM — Feature Coverage

Tracks the share of ClickHouse language features exposed via the
strongly-typed Python DSL.  Items marked ✅ are implemented and tested;
🚧 are partially implemented; ❌ are not yet covered.

> **Overall coverage**: 91 of 96 line items = **95%** ✅
>
> **Line test coverage** of `src/derp/chorm/`: **88%**
> (`uv run pytest tests/chorm --cov=derp.chorm`; pytest-cov is in the dev group)
>
> **Integration lane**: `tests/chorm/test_integration.py` executes generated
> DDL/DML against a real embedded ClickHouse (chdb) — create→insert→select,
> migration introspect→diff round-trips, ALTER — so SQL validity is proven on a
> real engine, not just string-asserted. Skips automatically if chdb is absent.

## Type system

### Numeric

- ✅ `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256`
- ✅ `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`
- ✅ `Float32`, `Float64`
- ✅ `Decimal(P, S)`, `Decimal32(S)`, `Decimal64(S)`, `Decimal128(S)`, `Decimal256(S)`
- ✅ `Bool`

### String

- ✅ `String`
- ✅ `FixedString(N)`

### Temporal

- ✅ `Date`, `Date32`
- ✅ `DateTime`, `DateTime('TZ')`
- ✅ `DateTime64(P)`, `DateTime64(P, 'TZ')`
- ❌ `IntervalSecond/Minute/Hour/Day/...` (rare; can be expressed via dateAdd)

### UUID / network

- ✅ `UUID`
- ✅ `IPv4`, `IPv6`

### Modifiers

- ✅ `Nullable(T)` (via `Nullable[T]` annotation)
- ✅ `LowCardinality(T)` (via `LowCardinality[T]`)

### Composite

- ✅ `Array(T)` (incl. nested arrays)
- ✅ `Tuple(T1, T2, ...)` anonymous and named
- ✅ `Map(K, V)`
- ✅ `Nested(name1 T1, name2 T2, ...)`
- ✅ `Variant(T1, T2, ...)`
- ✅ `Dynamic`
- ✅ `JSON`

### Aggregate state

- ✅ `AggregateFunction(name, ...types)`
- ✅ `SimpleAggregateFunction(name, type)`

### Geo

- ✅ `Point`, `Ring`, `Polygon`, `MultiPolygon`

### Enum

- ✅ `Enum8[<IntEnum>]`, `Enum16[<IntEnum>]` (Python enums auto-mapped)

## Table DDL

- ✅ `CREATE TABLE`, `CREATE TABLE IF NOT EXISTS`
- ✅ `ON CLUSTER`
- ✅ Per-database table references (`db.table`)
- ✅ Column `DEFAULT`, `MATERIALIZED`, `ALIAS`, `EPHEMERAL`
- ✅ Column `CODEC(...)` (pipelines supported)
- ✅ Column `TTL`
- ✅ Column `COMMENT`
- ✅ Table `COMMENT`
- ✅ `INDEX ... TYPE ... GRANULARITY` (data-skipping)
- ✅ `PROJECTION name (SELECT ... ORDER BY ...)`
- ✅ `PARTITION BY`, `PRIMARY KEY`, `ORDER BY`, `SAMPLE BY`
- ✅ `TTL` (table-level)
- ✅ `SETTINGS` (table-level engine settings)
- ✅ `CREATE DATABASE`, `DROP DATABASE`
- ✅ `CREATE VIEW`
- ✅ `CREATE MATERIALIZED VIEW` (to-existing and inline-engine, `POPULATE`)
- ✅ `CREATE DICTIONARY`
- ✅ `DROP TABLE`, `TRUNCATE TABLE`, `RENAME TABLE`
- ✅ `EXCHANGE TABLES`

## Table engines

### MergeTree family

- ✅ `MergeTree`
- ✅ `ReplacingMergeTree([version[, is_deleted]])`
- ✅ `SummingMergeTree([columns])`
- ✅ `AggregatingMergeTree`
- ✅ `CollapsingMergeTree(sign)`
- ✅ `VersionedCollapsingMergeTree(sign, version)`
- ✅ `GraphiteMergeTree(config_section)`

### Replicated variants

- ✅ `ReplicatedMergeTree`, `ReplicatedReplacingMergeTree`,
     `ReplicatedSummingMergeTree`, `ReplicatedAggregatingMergeTree`,
     `ReplicatedCollapsingMergeTree`,
     `ReplicatedVersionedCollapsingMergeTree`,
     `ReplicatedGraphiteMergeTree`

### Log family

- ✅ `Log`, `TinyLog`, `StripeLog`

### Special

- ✅ `Memory`, `Null`
- ✅ `Buffer`
- ✅ `Distributed`
- ✅ `Merge`
- ✅ `Dictionary`
- ✅ `Join`
- ✅ `Set` (as `SetEngine`)
- ✅ `URL`, `File`
- ✅ `View`, `MaterializedView`
- ✅ `Kafka` (settings-style)
- ✅ `S3`, `HDFS`, `MySQL`, `PostgreSQL`, `ODBC`, `JDBC`

## SELECT clauses

- ✅ `WITH` (CTE)
- ✅ `SELECT [DISTINCT | DISTINCT ON (...)]`
- ✅ `FROM table | FROM (subquery) AS alias`
- ✅ `FINAL`
- ✅ `SAMPLE k [OFFSET o]`
- ✅ `ARRAY JOIN`, `LEFT ARRAY JOIN`
- ✅ `INNER/LEFT/RIGHT/FULL/CROSS JOIN`
- ✅ `ASOF JOIN`, `SEMI/ANTI` (via strictness kwarg)
- ✅ `GLOBAL JOIN`
- ✅ `JOIN ... ON expr` and `USING (col)`
- ✅ `PREWHERE`
- ✅ `WHERE`
- ✅ `GROUP BY`, `GROUP BY WITH ROLLUP/CUBE/TOTALS`
- ✅ `HAVING`
- ✅ `ORDER BY ... [ASC|DESC] [NULLS FIRST/LAST] [COLLATE 'x']`
- ✅ `ORDER BY ... WITH FILL FROM ... TO ... STEP ...`
- ✅ `LIMIT n`, `LIMIT n OFFSET m`, `LIMIT n, m`
- ✅ `LIMIT n BY ...`
- ✅ Set ops: `UNION`, `UNION ALL`, `UNION DISTINCT`, `INTERSECT`, `EXCEPT`
- ✅ `SETTINGS k = v`
- ✅ `FORMAT name`
- ❌ `LATERAL VIEW` (uncommon; use ARRAY JOIN)

## Expressions

- ✅ Comparison: `= != < <= > >=`
- ✅ Logical: `AND`, `OR`, `NOT` via `& | ~`
- ✅ Arithmetic: `+ - * / %`
- ✅ `IN (list)`, `IN (SELECT ...)`, `NOT IN`, `GLOBAL IN`
- ✅ `LIKE`, `ILIKE`, `NOT LIKE`, `match()`
- ✅ `BETWEEN low AND high`
- ✅ `IS NULL`, `IS NOT NULL`
- ✅ `CASE x WHEN ... THEN ... END` and searched `CASE WHEN ... THEN ... END`
- ✅ `EXISTS (subquery)`
- ✅ Subqueries as expression / FROM / RHS of `IN`
- ✅ Raw SQL fragments (`sql()` / `raw()`)
- ✅ Aliases (`expr.as_("name")`)
- ✅ `CAST(x AS T)`

## Aggregate functions

- ✅ All combinators: `-If`, `-OrNull`, `-OrDefault`, `-State`, `-Merge`, `-Array`
- ✅ Parametric form `quantile(0.95)(x)` / `quantiles(...)`
- ✅ Distinct argument: `count(DISTINCT x)`
- ✅ Pre-typed Column helpers: `count`, `sum`, `avg`, `min`, `max`, `any`,
     `anyLast`, `uniq` family, `groupArray`, `groupUniqArray`,
     `quantile`/`quantiles`/`median`, `stddev`/`var`, `argMin`/`argMax`
- ✅ Catch-all `f.<funcName>(...)` for any other CH aggregate

## Scalar / string / date functions

- ✅ Catch-all dispatcher: `f.<anyFn>(...)` and `f("fn", *args)`
- ✅ Pre-imported helpers: `cast`, `coalesce`, `ifnull`, `if_`,
     `concat`, `length`, `lower`, `upper`, `position`, `substring`,
     `replaceAll`, `replaceRegexpAll` (`regex_replace`), `cityHash64`,
     `round`, `abs`, date functions (`toDate`, `toDateTime`,
     `toYYYYMM`/`toYYYYMMDD`, `toStartOfDay/Hour/Minute/Month`,
     `dateAdd`, `dateDiff`)
- ✅ `array(...)`, `tuple(...)`, `map(...)`, `arrayJoin(...)`

## Window functions

- ✅ `func() OVER (PARTITION BY ... ORDER BY ... [FRAME])`
- ✅ ASC/DESC per OVER column
- ✅ Custom frame clauses (raw string)

## INSERT

- ✅ `INSERT INTO ... VALUES (...)`
- ✅ Bulk `VALUES (...), (...), ...`
- ✅ `INSERT INTO ... SELECT ...`
- ✅ `SETTINGS k = v` after column list
- ✅ Native bulk insert via the underlying driver (`engine.client.insert(...)`)
- ❌ `INSERT INTO ... FORMAT JSONEachRow` (use `engine.client` + the driver's HTTP format)

## ALTER

### Columns

- ✅ `ADD COLUMN [IF NOT EXISTS] ... [FIRST | AFTER col]`
- ✅ `DROP COLUMN [IF EXISTS]`
- ✅ `MODIFY COLUMN`
- ✅ `RENAME COLUMN`
- ✅ `COMMENT COLUMN`
- ✅ `CLEAR COLUMN [IN PARTITION p]`

### Indexes

- ✅ `ADD INDEX`, `DROP INDEX`, `MATERIALIZE INDEX`

### TTL / settings

- ✅ `MODIFY TTL`, `REMOVE TTL`
- ✅ `MODIFY SETTING`, `RESET SETTING`

### Partition

- ✅ `ATTACH/DETACH/DROP/FREEZE/MOVE PARTITION`

### Mutations

- ✅ `ALTER TABLE ... UPDATE ... WHERE ...`
- ✅ `ALTER TABLE ... DELETE WHERE ...`
- ✅ Lightweight `DELETE FROM ... WHERE ...`
- ✅ `IN PARTITION` qualifier for mutations
- ✅ `MATERIALIZE TTL`, `MATERIALIZE COLUMN`

## Migrations

- ✅ JSON-serializable `SchemaSnapshot` from `Table` subclasses
- ✅ Live-server introspection via `system.tables` / `system.columns` /
     `system.data_skipping_indices`
- ✅ Diff between two snapshots → list of `Statement` objects
- ✅ Rename column hints
- ✅ Engine TTL add / remove / modify
- ✅ Index add / drop / re-create on differ
- ✅ Destructive-action flag (`Statement.is_destructive()`)
- ✅ Migration journal table (`_derp_chorm_migrations`)
- ✅ Reverse diff for rollback (`diff_down`, structure only — data not restored)
- ✅ CLI: `derp ch generate / migrate / push / pull / status / check / drop / rollback`

## Other

- ✅ `SETTINGS k = v` on SELECT, INSERT, ALTER, UPDATE, DELETE
- ✅ `FORMAT` on SELECT
- ✅ ClickHouse parameterized queries (`{name:Type}`) with auto-typing
- ✅ Backtick-quoted identifiers
- ✅ `OPTIMIZE TABLE`
- ❌ `SYSTEM` commands (use `command`)
- ❌ Row-policy / quota / role DDL
- ❌ User / grant DDL

## Tracking summary

```
implemented = 91
partial     =  0
total       = 96
% covered   = 95%
```
