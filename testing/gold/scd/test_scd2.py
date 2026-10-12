from datetime import datetime

import pytest

from gold.scd.scd2 import Scd2Table, merge_scd2, validate_scd2
from pipeline.errors import DataValidationError

TABLE = Scd2Table(name="dim_t", business_key="k", surrogate_key="t_sk", attributes=("name", "amount"))


@pytest.fixture
def dim(isolated_spark):
    name = "scd2_test_dim"
    isolated_spark.sql(f"DROP TABLE IF EXISTS {name}")
    isolated_spark.sql(f"""
        CREATE TABLE {name} (
            t_sk BIGINT NOT NULL, k STRING NOT NULL, name STRING, amount INT,
            attribute_hash STRING NOT NULL,
            effective_start_date TIMESTAMP NOT NULL, effective_end_date TIMESTAMP NOT NULL,
            is_current BOOLEAN NOT NULL, row_version INT NOT NULL,
            _source_batch_id STRING, _pipeline_run_id STRING, _gold_loaded_timestamp TIMESTAMP
        ) USING DELTA
    """)
    return name


def _increment(spark, rows):
    """rows: (k, name, amount, changed_at, created_at). Shaped like a silver increment."""
    return spark.createDataFrame(
        rows,
        "k string, name string, amount int, _change_ts timestamp, _first_change_ts timestamp",
    ).selectExpr("*", "'batch' AS _source_batch_id")


def _current(spark, name):
    return {r["k"]: r for r in spark.table(name).where("is_current").collect()}


def test_new_keys_open_version_one(isolated_spark, dim):
    rows = _increment(isolated_spark, [
        ("a", "Ann", 5, datetime(2024, 2, 1), datetime(2024, 1, 1)),
        ("b", "Bo", 7, datetime(2024, 2, 2), datetime(2024, 1, 2)),
    ])
    result = merge_scd2(isolated_spark, dim, TABLE, rows, run_id="run1")
    assert (result.new_keys, result.changed_keys, result.unchanged_rows) == (2, 0, 0)
    assert (result.versions_inserted, result.versions_closed) == (2, 0)
    a = _current(isolated_spark, dim)["a"]
    assert a["row_version"] == 1
    assert a["effective_start_date"] == datetime(2024, 1, 1)     # created_at for a new key
    assert a["effective_end_date"] == datetime(9999, 12, 31)
    assert a["_pipeline_run_id"] == "run1"


def test_change_closes_old_version_and_opens_next(isolated_spark, dim):
    merge_scd2(isolated_spark, dim, TABLE, _increment(isolated_spark, [
        ("a", "Ann", 5, datetime(2024, 2, 1), datetime(2024, 1, 1)),
        ("b", "Bo", 7, datetime(2024, 2, 2), datetime(2024, 1, 2)),
    ]), run_id="run1")

    rows = _increment(isolated_spark, [
        ("a", "Ann", 5, datetime(2024, 2, 1), datetime(2024, 1, 1)),   # unchanged, rewritten by the source
        ("b", "Bo", 9, datetime(2024, 3, 1), datetime(2024, 1, 2)),    # amount changed
    ])
    result = merge_scd2(isolated_spark, dim, TABLE, rows, run_id="run2")
    assert (result.new_keys, result.changed_keys, result.unchanged_rows, result.stale_rows) == (0, 1, 1, 0)
    assert (result.versions_inserted, result.versions_closed) == (1, 1)

    history = [r for r in isolated_spark.table(dim).where("k = 'b'").orderBy("row_version").collect()]
    assert [r["row_version"] for r in history] == [1, 2]
    assert history[0]["effective_end_date"] == datetime(2024, 3, 1)   # old version ends where the new one starts
    assert history[0]["is_current"] is False
    assert history[1]["effective_start_date"] == datetime(2024, 3, 1)
    assert history[1]["amount"] == 9 and history[1]["is_current"] is True
    assert validate_scd2(isolated_spark, dim, TABLE) == {
        "keys_with_multiple_current": 0, "keys_without_current": 0, "version_gaps_or_overlaps": 0,
    }


def test_change_older_than_the_current_version_is_stale_and_ignored(isolated_spark, dim):
    # A new key starts at created_at (Jan 2). A change dated before that cannot be applied after it.
    merge_scd2(isolated_spark, dim, TABLE, _increment(isolated_spark, [
        ("b", "Bo", 9, datetime(2024, 3, 1), datetime(2024, 1, 2)),
    ]), run_id="run1")
    late = _increment(isolated_spark, [("b", "Bo", 1, datetime(2024, 1, 1), datetime(2024, 1, 2))])
    result = merge_scd2(isolated_spark, dim, TABLE, late, run_id="run2")
    assert (result.stale_rows, result.versions_inserted, result.versions_closed) == (1, 0, 0)
    assert _current(isolated_spark, dim)["b"]["amount"] == 9


def test_rerunning_the_same_increment_writes_nothing(isolated_spark, dim):
    rows = _increment(isolated_spark, [("a", "Ann", 5, datetime(2024, 2, 1), datetime(2024, 1, 1))])
    merge_scd2(isolated_spark, dim, TABLE, rows, run_id="run1")
    again = merge_scd2(isolated_spark, dim, TABLE, rows, run_id="run2")
    assert (again.new_keys, again.changed_keys, again.unchanged_rows) == (0, 0, 1)
    assert (again.versions_inserted, again.versions_closed) == (0, 0)
    assert isolated_spark.table(dim).count() == 1


def test_missing_change_timestamp_is_rejected(isolated_spark, dim):
    rows = isolated_spark.createDataFrame(
        [("a", "Ann", 5, None, datetime(2024, 1, 1))],
        "k string, name string, amount int, _change_ts timestamp, _first_change_ts timestamp",
    ).selectExpr("*", "'batch' AS _source_batch_id")
    with pytest.raises(DataValidationError, match="change timestamp"):
        merge_scd2(isolated_spark, dim, TABLE, rows, run_id="run1")
