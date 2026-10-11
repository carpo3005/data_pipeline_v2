import json

import pytest
from pyspark.sql import SparkSession

from pipeline import events
from pipeline.audit import AuditRecord, AuditRun, combine_sinks, make_delta_sink
from pipeline.catalog import ensure_schema_exists, get_table_fqn, get_schema_fqn
from pipeline.events import EventBus

CATALOG = "forge"
TEST_SCHEMA = "test_audit"
AUDIT_TABLE = "audit_runs"


def test_success_records_metrics():
    records = []
    with AuditRun("bronze", "customer", "t", sink=records.append) as run:
        run.add("source_row_count", 5)
    assert records[0].status == "success"
    assert records[0].metrics == {"source_row_count": 5}
    assert records[0].completed_at is not None


def test_failure_records_error_and_reraises():
    records = []
    with pytest.raises(ValueError, match="boom"):
        with AuditRun("bronze", "customer", "t", sink=records.append):
            raise ValueError("boom")
    assert records[0].status == "failed"
    assert records[0].error_message == "boom"


def _failing_sink(record: AuditRecord) -> None:
    raise RuntimeError("sink down")


def test_failing_sink_does_not_mask_run_error():
    with pytest.raises(ValueError, match="boom") as info:
        with AuditRun("bronze", "customer", "t", sink=_failing_sink):
            raise ValueError("boom")
    assert "Audit sink also failed: RuntimeError: sink down" in info.value.__notes__


def test_failing_sink_raises_when_run_succeeded():
    with pytest.raises(RuntimeError, match="sink down"):
        with AuditRun("bronze", "customer", "t", sink=_failing_sink):
            pass


def test_run_publishes_started_and_succeeded_events():
    bus = EventBus()
    seen = []
    bus.subscribe(seen.append)
    with AuditRun("bronze", "customer", "t", sink=lambda r: None, bus=bus) as run:
        pass
    assert [e.name for e in seen] == [events.RUN_STARTED, events.RUN_SUCCEEDED]
    assert seen[1].payload["record"] is run.record
    assert run.record.status == "success"


def test_run_publishes_failed_event_and_reraises():
    bus = EventBus()
    seen = []
    bus.subscribe(seen.append, names=[events.RUN_FAILED])
    with pytest.raises(ValueError, match="boom"):
        with AuditRun("bronze", "customer", "t", sink=lambda r: None, bus=bus):
            raise ValueError("boom")
    assert seen[0].payload["record"].error_message == "boom"


def test_failing_subscriber_does_not_mask_run_error():
    bus = EventBus()

    def broken(event):
        raise RuntimeError("observer down")

    bus.subscribe(broken)
    with pytest.raises(ValueError, match="boom"):
        with AuditRun("bronze", "customer", "t", sink=lambda r: None, bus=bus):
            raise ValueError("boom")


def test_combine_sinks_sends_record_to_every_sink():
    first, second = [], []
    with AuditRun("bronze", "customer", "t", sink=combine_sinks(first.append, second.append)):
        pass
    assert len(first) == 1
    assert first == second


# --------------------------------------------------------------------------------
# Delta sink
# --------------------------------------------------------------------------------


@pytest.fixture
def audit_table(spark: SparkSession):
    """Provide a throwaway audit table and drop its schema afterwards."""
    ensure_schema_exists(spark, CATALOG, TEST_SCHEMA)
    table_fqn = get_table_fqn(CATALOG, TEST_SCHEMA, AUDIT_TABLE)
    yield table_fqn
    spark.sql(f"DROP SCHEMA IF EXISTS {get_schema_fqn(CATALOG, TEST_SCHEMA)} CASCADE")


def test_make_delta_sink_writes_record_to_table(spark: SparkSession, audit_table):
    sink = make_delta_sink(spark, audit_table)
    with AuditRun("bronze", "customer", "forge.bronze.customer", sink=sink) as run:
        run.add("source_row_count", 5)
        run.add("source_file", "customer.csv")

    rows = spark.read.table(audit_table).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.run_id == run.record.run_id
    assert row.stage == "bronze"
    assert row.entity == "customer"
    assert row.target_table == "forge.bronze.customer"
    assert row.status == "success"
    assert row.completed_at is not None
    assert row.error_message is None
    assert json.loads(row.metrics) == {"source_row_count": 5, "source_file": "customer.csv"}


def test_make_delta_sink_records_failed_run(spark: SparkSession, audit_table):
    sink = make_delta_sink(spark, audit_table)
    with pytest.raises(ValueError, match="boom"):
        with AuditRun("bronze", "customer", "t", sink=sink):
            raise ValueError("boom")

    row = spark.read.table(audit_table).collect()[0]
    assert row.status == "failed"
    assert row.error_message == "boom"


def test_make_delta_sink_appends_each_run(spark: SparkSession, audit_table):
    sink = make_delta_sink(spark, audit_table)
    for entity in ["customer", "address"]:
        with AuditRun("bronze", entity, "t", sink=sink):
            pass

    entities = sorted(r.entity for r in spark.read.table(audit_table).collect())
    assert entities == ["address", "customer"]
