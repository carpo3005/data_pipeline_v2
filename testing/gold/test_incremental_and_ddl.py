from datetime import datetime

from gold.ddl import is_informational_alter, split_statements, strip_informational_constraints
from gold.incremental import CHANGE_TS, incremental_rows, last_watermark, next_watermark, with_change_timestamps
from pipeline.audit import AuditRecord, make_delta_sink


def test_change_timestamps_fall_back_to_the_other_timestamp(isolated_spark):
    df = isolated_spark.createDataFrame(
        [(datetime(2024, 1, 1), datetime(2024, 2, 1)), (None, datetime(2024, 3, 1))],
        "created_at timestamp, updated_at timestamp",
    )
    rows = with_change_timestamps(df).orderBy("updated_at").collect()
    assert rows[0]["_change_ts"] == datetime(2024, 2, 1)
    assert rows[0]["_first_change_ts"] == datetime(2024, 1, 1)
    assert rows[1]["_change_ts"] == datetime(2024, 3, 1)
    assert rows[1]["_first_change_ts"] == datetime(2024, 3, 1)


def test_incremental_rows_keeps_equal_timestamps_and_handles_no_watermark(isolated_spark):
    df = isolated_spark.createDataFrame([(datetime(2024, 1, 1),), (datetime(2024, 2, 1),)], "_change_ts timestamp")
    assert incremental_rows(df, None).count() == 2
    assert incremental_rows(df, datetime(2024, 2, 1)).count() == 1


def test_next_watermark_only_moves_forward(isolated_spark):
    df = isolated_spark.createDataFrame([(datetime(2024, 5, 1),)], CHANGE_TS + " timestamp")
    assert next_watermark(None, df) == datetime(2024, 5, 1)
    assert next_watermark(datetime(2024, 6, 1), df) == datetime(2024, 6, 1)
    empty = df.where("1 = 0")
    assert next_watermark(datetime(2024, 6, 1), empty) == datetime(2024, 6, 1)


def test_last_watermark_reads_the_latest_successful_gold_load(isolated_spark):
    audit = "audit_test.gold_load_audit"
    isolated_spark.sql("CREATE SCHEMA IF NOT EXISTS audit_test")
    sink = make_delta_sink(isolated_spark, audit)
    target = "gold.dim_x"

    def record(started, status, watermark):
        sink(AuditRecord(run_id=str(started), stage="gold_load", entity="x", target_table=target, status=status,
                         started_at=started, completed_at=started, metrics={"high_watermark": watermark}))

    assert last_watermark(isolated_spark, audit, target) is None
    record(datetime(2024, 1, 1), "success", "2024-01-05 00:00:00")
    record(datetime(2024, 1, 2), "success", "2024-01-09 12:30:00")
    record(datetime(2024, 1, 3), "failed", "2024-02-01 00:00:00")   # a failed run must not move the watermark
    assert last_watermark(isolated_spark, audit, target) == datetime(2024, 1, 9, 12, 30)
    assert last_watermark(isolated_spark, audit, "gold.other") is None


def test_split_statements_drops_comments_and_empty_pieces():
    script = """
    -- heading only
    CREATE TABLE a (x INT);   -- trailing comment
    ;
    INSERT INTO a VALUES (1);
    """
    assert split_statements(script) == ["CREATE TABLE a (x INT)", "INSERT INTO a VALUES (1)"]


def test_informational_constraints_are_removed_but_checks_are_kept():
    statement = """CREATE OR REPLACE TABLE gold.t
    (
        sk BIGINT NOT NULL,
        amount DECIMAL(18,2),
        CONSTRAINT t_pk PRIMARY KEY (sk),
        CONSTRAINT t_fk FOREIGN KEY (amount) REFERENCES gold.u(amount),
        CONSTRAINT t_ck CHECK (amount >= 0)
    )
    USING DELTA"""
    stripped = strip_informational_constraints(statement)
    assert "PRIMARY KEY" not in stripped and "FOREIGN KEY" not in stripped
    assert "DECIMAL(18,2)" in stripped and "CHECK (amount >= 0)" in stripped
    assert stripped.rstrip().endswith("USING DELTA")


def test_informational_alter_is_recognised_and_check_alter_is_not():
    assert is_informational_alter("ALTER TABLE gold.t ADD CONSTRAINT t_bk UNIQUE (id)")
    assert not is_informational_alter("ALTER TABLE gold.t ADD CONSTRAINT ck CHECK (id >= 0)")


def test_increment_with_repairs_adds_old_facts_that_still_point_at_unknown(isolated_spark):
    from gold.incremental import increment_with_repairs
    from gold.keys import UNKNOWN_KEY

    source = isolated_spark.createDataFrame(
        [("o1", 10, datetime(2024, 1, 1)), ("o2", 20, datetime(2024, 3, 1))],
        "order_item_id string, customer_sk bigint, _change_ts timestamp",
    )
    target = isolated_spark.createDataFrame(
        [("o1", UNKNOWN_KEY), ("o2", 20)], "order_item_id string, customer_sk bigint"
    )
    # Watermark after both rows: o1 is old, so only the repair brings it back; o2 is old and already loaded
    rows = increment_with_repairs(source, datetime(2024, 4, 1), target, "order_item_id", ["customer_sk"])
    assert sorted(r["order_item_id"] for r in rows.collect()) == ["o1"]


def test_watermark_is_ignored_when_the_target_was_recreated(isolated_spark):
    audit = "audit_test.gold_load_audit"
    isolated_spark.sql("CREATE SCHEMA IF NOT EXISTS audit_test")
    isolated_spark.sql("DROP TABLE IF EXISTS gold_reset_test")
    isolated_spark.sql("CREATE TABLE gold_reset_test (customer_id STRING, v INT) USING DELTA")
    isolated_spark.sql("INSERT INTO gold_reset_test VALUES ('-1', 0)")          # seed row only: the target was recreated
    sink = make_delta_sink(isolated_spark, audit)
    sink(AuditRecord(run_id="r", stage="gold_load", entity="x", target_table="gold_reset_test", status="success",
                     started_at=datetime(2024, 1, 1), completed_at=datetime(2024, 1, 1),
                     metrics={"high_watermark": "2024-01-05 00:00:00"}))
    assert last_watermark(isolated_spark, audit, "gold_reset_test") == datetime(2024, 1, 5)
    assert last_watermark(isolated_spark, audit, "gold_reset_test", "customer_id") is None
    isolated_spark.sql("INSERT INTO gold_reset_test VALUES ('c1', 1)")
    assert last_watermark(isolated_spark, audit, "gold_reset_test", "customer_id") == datetime(2024, 1, 5)
