from datetime import date, datetime

from gold.keys import UNKNOWN_KEY, attribute_hash, date_key, surrogate_key
from gold.lookup import count_unknown, key_lookup, keys_with_unknown_member, point_in_time_key
from pyspark.sql import functions as F


def _values(df, *columns):
    return [tuple(r) for r in df.select(*columns).collect()]


def test_surrogate_key_is_deterministic_and_depends_on_inputs(isolated_spark):
    df = isolated_spark.createDataFrame([("a", 1), ("a", 2), ("a", 1)], "k string, n int")
    keys = [r[0] for r in df.select(surrogate_key("k", "n").alias("sk")).collect()]
    assert keys[0] == keys[2]
    assert keys[0] != keys[1]


def test_date_key_is_yyyymmdd(isolated_spark):
    df = isolated_spark.createDataFrame([(datetime(2024, 1, 31, 23, 59),)], "ts timestamp")
    assert _values(df, date_key("ts")) == [(20240131,)]


def test_attribute_hash_separates_null_from_empty_and_respects_order(isolated_spark):
    df = isolated_spark.createDataFrame(
        [("x", None), ("x", ""), ("x", None), ("y", None)], "a string, b string"
    )
    hashes = [r[0] for r in df.select(attribute_hash(["a", "b"]).alias("h")).collect()]
    assert hashes[0] == hashes[2]          # same values
    assert hashes[0] != hashes[1]          # NULL is not the same as an empty string
    swapped = isolated_spark.createDataFrame([("x", "y")], "a string, b string")
    original = isolated_spark.createDataFrame([("y", "x")], "a string, b string")
    assert swapped.select(attribute_hash(["a", "b"])).first()[0] != original.select(attribute_hash(["a", "b"])).first()[0]


def test_point_in_time_key_picks_the_version_valid_at_the_fact_time(isolated_spark):
    dims = isolated_spark.createDataFrame(
        [
            ("A", 100, datetime(2024, 1, 1), datetime(2024, 6, 1)),
            ("A", 101, datetime(2024, 6, 1), datetime(9999, 12, 31)),
        ],
        "cust string, sk bigint, effective_start_date timestamp, effective_end_date timestamp",
    )
    facts = isolated_spark.createDataFrame(
        [
            (1, "A", datetime(2023, 12, 31)),   # before the first version
            (2, "A", datetime(2024, 1, 1)),     # exactly the start of version 1
            (3, "A", datetime(2024, 6, 1)),     # exactly the start of version 2 (end of version 1 is exclusive)
            (4, "A", None),                     # no time: unknown member
            (5, "B", datetime(2024, 3, 1)),     # no such customer
        ],
        "id int, cust string, t timestamp",
    )
    out = point_in_time_key(facts, dims, fact_key="cust", dim_key="cust", fact_time="t",
                            dim_sk="sk", output="customer_sk")
    assert _values(out.orderBy("id"), "id", "customer_sk") == [
        (1, UNKNOWN_KEY), (2, 100), (3, 101), (4, UNKNOWN_KEY), (5, UNKNOWN_KEY),
    ]


def test_key_lookup_and_unknown_counts(isolated_spark):
    dates = isolated_spark.createDataFrame([(20240101, 7)], "date_key int, date_sk bigint")
    facts = isolated_spark.createDataFrame([(20240101,), (20991231,)], "dk int")
    out = key_lookup(facts, dates, fact_key="dk", dim_key="date_key", dim_sk="date_sk", output="date_sk_out")
    assert _values(out.orderBy("dk"), "dk", "date_sk_out") == [(20240101, 7), (20991231, UNKNOWN_KEY)]
    assert count_unknown(out, ["date_sk_out"]) == {"unknown_date_sk_out_count": 1}


def test_keys_with_unknown_member_finds_facts_to_repair(isolated_spark):
    target = isolated_spark.createDataFrame(
        [("o1", 1, 2), ("o2", UNKNOWN_KEY, 2), ("o3", 1, UNKNOWN_KEY)], "id string, a int, b int"
    )
    repairs = keys_with_unknown_member(target, "id", ["a", "b"])
    assert sorted(r[0] for r in repairs.collect()) == ["o2", "o3"]
