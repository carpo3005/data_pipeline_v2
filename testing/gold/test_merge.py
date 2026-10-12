import pytest

from gold.merge import merge_upsert
from pipeline.errors import DataValidationError


def _target(spark, name):
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(f"CREATE TABLE {name} (k STRING, sk BIGINT, v INT, h STRING, _gold_loaded_timestamp TIMESTAMP) USING DELTA")
    return name


def test_merge_upsert_inserts_updates_and_leaves_unchanged_rows(isolated_spark):
    target = _target(isolated_spark, "merge_test_upsert")
    isolated_spark.sql(f"INSERT INTO {target} VALUES ('a', 1, 10, 'h1', NULL), ('b', 2, 20, 'h2', NULL)")
    incoming = isolated_spark.createDataFrame(
        [("a", 1, 10, "h1"), ("b", 2, 21, "h2b"), ("c", 3, 30, "h3")], "k string, sk bigint, v int, h string"
    ).selectExpr("*", "CAST(NULL AS TIMESTAMP) AS _gold_loaded_timestamp")

    result = merge_upsert(isolated_spark, target, incoming, business_key="k", changed_columns=("v", "h"))
    assert (result.inserted, result.updated, result.unchanged) == (1, 1, 1)
    assert sorted(tuple(r) for r in isolated_spark.table(target).select("k", "v").collect()) == [
        ("a", 10), ("b", 21), ("c", 30),
    ]


def test_merge_upsert_rejects_duplicate_keys(isolated_spark):
    target = _target(isolated_spark, "merge_test_dupes")
    incoming = isolated_spark.createDataFrame(
        [("a", 1, 1, "h"), ("a", 1, 2, "h")], "k string, sk bigint, v int, h string"
    ).selectExpr("*", "CAST(NULL AS TIMESTAMP) AS _gold_loaded_timestamp")
    with pytest.raises(DataValidationError, match="duplicate"):
        merge_upsert(isolated_spark, target, incoming, business_key="k", changed_columns=("v",))
