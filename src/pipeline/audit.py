from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from pipeline.writer import append_table

Metric = int | float | str
Sink = Callable[["AuditRecord"], None]


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class AuditRecord:
    run_id: str
    stage: str # e.g., "bronze", "silver", "gold"
    entity: str
    target_table: str
    status: str                      # "running" | "success" | "failed"
    started_at: datetime
    completed_at: datetime | None = None
    error_message: str | None = None
    metrics: dict[str, Metric] = field(default_factory=dict)


class AuditRun:
    """Records one run. Emits exactly one AuditRecord to the sink on exit."""

    def __init__(
        self,
        stage: str,
        entity: str,
        target_table: str,
        sink: Sink,
        run_id: str | None = None,
    ):
        self._sink = sink
        self.record = AuditRecord(
            run_id=run_id or str(uuid.uuid4()),
            stage=stage,
            entity=entity,
            target_table=target_table,
            status="running",
            started_at=_now(),
        )

    def add(self, name: str, value: Metric) -> None:
        self.record.metrics[name] = value

    def __enter__(self) -> AuditRun:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.record.completed_at = _now()
        if exc is None:
            self.record.status = "success"
        else:
            self.record.status = "failed"
            self.record.error_message = str(exc)
        try:
            self._sink(self.record)
        except Exception as sink_exc:
            if exc is None:
                raise
            # Keep the run's own error as the one raised; record the sink failure on it
            exc.add_note(f"Audit sink also failed: {type(sink_exc).__name__}: {sink_exc}")
        return False  # never swallow the exception


# --------------------------------------------------------------------------------
# Sinks
# --------------------------------------------------------------------------------


AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("stage", StringType(), False),
    StructField("entity", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("status", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), True),
    StructField("error_message", StringType(), True),
    StructField("metrics", StringType(), False),   # JSON
])


def make_delta_sink(spark: SparkSession, table_fqn: str) -> Sink:
    """Return a sink that appends each AuditRecord as a row to table_fqn."""
    def sink(record: AuditRecord) -> None:
        row = asdict(record)
        row["metrics"] = json.dumps(row["metrics"])
        df = spark.createDataFrame([row], schema=AUDIT_SCHEMA)
        append_table(df, table_fqn)
    return sink


def combine_sinks(*sinks: Sink) -> Sink:
    """Return a sink that sends each record to every sink in order."""
    def sink(record: AuditRecord) -> None:
        for s in sinks:
            s(record)
    return sink
