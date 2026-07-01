"""Tests for ClickHouse table-engine specifications."""

from __future__ import annotations

from derp.chorm import (
    HDFS,
    JDBC,
    ODBC,
    S3,
    URL,
    AggregatingMergeTree,
    Buffer,
    CollapsingMergeTree,
    Dictionary,
    Distributed,
    File,
    GraphiteMergeTree,
    Join,
    KafkaEngine,
    Log,
    Memory,
    Merge,
    MergeTree,
    MySQL,
    Null,
    PostgreSQL,
    ReplacingMergeTree,
    ReplicatedMergeTree,
    ReplicatedReplacingMergeTree,
    SetEngine,
    StripeLog,
    SummingMergeTree,
    TinyLog,
    VersionedCollapsingMergeTree,
)


def test_merge_tree_default():
    e = MergeTree(order_by="tuple()")
    assert e.engine_clause() == "ENGINE = MergeTree()"
    assert e.order_by_clause() == "ORDER BY tuple()"
    assert e.partition_by_clause() is None


def test_merge_tree_full_clauses():
    e = MergeTree(
        order_by=("user_id", "ts"),
        partition_by="toYYYYMM(ts)",
        primary_key="user_id",
        sample_by="user_id",
        ttl="ts + INTERVAL 30 DAY",
        settings={"index_granularity": 8192, "min_bytes_for_wide_part": 0},
    )
    assert e.order_by_clause() == "ORDER BY (user_id, ts)"
    assert e.partition_by_clause() == "PARTITION BY toYYYYMM(ts)"
    assert e.primary_key_clause() == "PRIMARY KEY user_id"
    assert e.sample_by_clause() == "SAMPLE BY user_id"
    assert e.ttl_clause() == "TTL ts + INTERVAL 30 DAY"
    s = e.settings_clause()
    assert s is not None
    assert "index_granularity = 8192" in s
    assert "min_bytes_for_wide_part = 0" in s


def test_replacing_merge_tree():
    e = ReplacingMergeTree("version", order_by="id")
    assert e.engine_clause() == "ENGINE = ReplacingMergeTree(version)"


def test_replacing_merge_tree_with_is_deleted():
    e = ReplacingMergeTree("version", "is_deleted", order_by="id")
    assert e.engine_clause() == "ENGINE = ReplacingMergeTree(version, is_deleted)"


def test_replacing_merge_tree_no_version():
    e = ReplacingMergeTree(order_by="id")
    assert e.engine_clause() == "ENGINE = ReplacingMergeTree()"


def test_summing_merge_tree():
    e = SummingMergeTree(("a", "b"), order_by="id")
    assert "SummingMergeTree" in e.engine_clause()
    assert "(a, b)" in e.engine_clause()


def test_aggregating_merge_tree():
    e = AggregatingMergeTree(order_by="id")
    assert e.engine_clause() == "ENGINE = AggregatingMergeTree()"


def test_collapsing_merge_tree():
    e = CollapsingMergeTree("Sign", order_by="id")
    assert e.engine_clause() == "ENGINE = CollapsingMergeTree(Sign)"


def test_versioned_collapsing_merge_tree():
    e = VersionedCollapsingMergeTree("Sign", "Version", order_by="id")
    assert "VersionedCollapsingMergeTree(Sign, Version)" in e.engine_clause()


def test_graphite_merge_tree():
    e = GraphiteMergeTree("graphite_rollup", order_by="id")
    assert e.engine_clause() == "ENGINE = GraphiteMergeTree('graphite_rollup')"


def test_replicated_merge_tree():
    e = ReplicatedMergeTree(
        "/clickhouse/tables/{shard}/x",
        "{replica}",
        order_by="id",
    )
    cl = e.engine_clause()
    assert "ReplicatedMergeTree" in cl
    assert "'/clickhouse/tables/{shard}/x'" in cl
    assert "'{replica}'" in cl


def test_replicated_replacing_merge_tree_with_version():
    e = ReplicatedReplacingMergeTree("/path/x", "{replica}", version="v", order_by="id")
    assert "ReplicatedReplacingMergeTree" in e.engine_clause()
    assert ", v)" in e.engine_clause()


def test_log_family_engines():
    for E in (Log, TinyLog, StripeLog):
        e = E()
        assert e.engine_clause() == f"ENGINE = {E.__name__}"


def test_memory_null():
    assert Memory().engine_clause() == "ENGINE = Memory()"
    assert Null().engine_clause() == "ENGINE = Null()"


def test_buffer_engine():
    e = Buffer("db", "tbl", num_layers=8)
    cl = e.engine_clause()
    assert cl.startswith("ENGINE = Buffer(")
    assert "db, tbl, 8" in cl


def test_distributed_engine():
    e = Distributed("my_cluster", "db", "tbl", "rand()", "policy_a")
    cl = e.engine_clause()
    assert cl == "ENGINE = Distributed(my_cluster, db, tbl, rand(), 'policy_a')"


def test_merge_engine():
    e = Merge("db", r"events_\d+")
    assert e.engine_clause() == r"ENGINE = Merge(db, 'events_\d+')"


def test_dictionary_engine():
    e = Dictionary("dict_name")
    assert e.engine_clause() == "ENGINE = Dictionary(dict_name)"


def test_join_engine():
    e = Join("ALL", "LEFT", "user_id")
    assert e.engine_clause() == "ENGINE = Join(ALL, LEFT, user_id)"


def test_set_engine():
    assert SetEngine().engine_clause() == "ENGINE = Set()"


def test_url_engine():
    e = URL("https://example.com", "JSONEachRow")
    assert e.engine_clause() == ("ENGINE = URL('https://example.com', JSONEachRow)")


def test_file_engine():
    assert File("JSON").engine_clause() == "ENGINE = File(JSON)"
    e2 = File("CSV", "/tmp/data.csv")
    assert "/tmp/data.csv" in e2.engine_clause()


def test_kafka_engine_settings():
    e = KafkaEngine(
        broker_list="localhost:9092",
        topic_list="my_topic",
        group_name="my_group",
        format="JSONEachRow",
    )
    assert e.engine_clause() == "ENGINE = Kafka()"
    s = e.settings_clause()
    assert s is not None
    assert "kafka_broker_list = 'localhost:9092'" in s
    assert "kafka_topic_list = 'my_topic'" in s
    assert "kafka_group_name = 'my_group'" in s
    assert "kafka_format = 'JSONEachRow'" in s


# -- Integration engines -----------------------------------------------------


def test_s3_engine_minimal():
    e = S3("https://bucket.s3.amazonaws.com/path/*.csv", "CSV")
    assert (
        e.engine_clause()
        == "ENGINE = S3('https://bucket.s3.amazonaws.com/path/*.csv', 'CSV')"
    )


def test_s3_engine_with_credentials_and_compression():
    e = S3(
        "https://bucket.s3.amazonaws.com/path/*.csv.gz",
        "CSV",
        aws_access_key_id="AKIA",
        aws_secret_access_key="secret",
        compression="gzip",
    )
    assert e.engine_clause() == (
        "ENGINE = S3('https://bucket.s3.amazonaws.com/path/*.csv.gz', "
        "'AKIA', 'secret', 'CSV', 'gzip')"
    )


def test_hdfs_engine():
    e = HDFS("hdfs://namenode:9000/path/file", "Parquet")
    assert (
        e.engine_clause()
        == "ENGINE = HDFS('hdfs://namenode:9000/path/file', 'Parquet')"
    )


def test_mysql_engine_minimal():
    e = MySQL("mysql:3306", "db", "users", "root", "pw")
    assert (
        e.engine_clause() == "ENGINE = MySQL('mysql:3306', 'db', 'users', 'root', 'pw')"
    )


def test_mysql_engine_replace_and_on_duplicate():
    e = MySQL(
        "mysql:3306",
        "db",
        "users",
        "root",
        "pw",
        replace_query=True,
        on_duplicate_clause="UPDATE c = c + 1",
    )
    assert e.engine_clause() == (
        "ENGINE = MySQL('mysql:3306', 'db', 'users', 'root', 'pw', "
        "1, 'UPDATE c = c + 1')"
    )


def test_postgresql_engine():
    e = PostgreSQL("pg:5432", "db", "users", "postgres", "pw", schema="public")
    assert e.engine_clause() == (
        "ENGINE = PostgreSQL('pg:5432', 'db', 'users', 'postgres', 'pw', 'public')"
    )


def test_odbc_engine():
    e = ODBC("DSN=mydsn", "external_db", "external_table")
    assert (
        e.engine_clause()
        == "ENGINE = ODBC('DSN=mydsn', 'external_db', 'external_table')"
    )


def test_jdbc_engine():
    e = JDBC("jdbc:mysql://host:3306/?user=root", "external_db", "external_table")
    assert e.engine_clause() == (
        "ENGINE = JDBC('jdbc:mysql://host:3306/?user=root', "
        "'external_db', 'external_table')"
    )
