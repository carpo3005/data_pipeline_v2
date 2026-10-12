from collections.abc import Sequence
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold.lookup import keys_with_unknown_member

CHANGE_TS = "_change_ts"              # when the source row last changed (updated_at, else created_at)
FIRST_CHANGE_TS = "_first_change_ts"  # when the source row first existed (created_at, else updated_at)
GOLD_STAGE = "gold_load"              # AuditRun stage for every gold load
WATERMARK_METRIC = "high_watermark"   # audit metric holding the newest change timestamp a load processed


def with_change_timestamps(df: DataFrame, updated: str = "updated_at", created: str = "created_at") -> DataFrame:
    return df.withColumns({
        CHANGE_TS: F.coalesce(F.col(updated), F.col(created)),
        FIRST_CHANGE_TS: F.coalesce(F.col(created), F.col(updated)),
    })


def last_watermark(
    spark: SparkSession,
    audit_table: str,
    target_table: str,
    business_key: str | None = None,
) -> datetime | None:
    """Newest change timestamp recorded by a successful gold load of target_table, or None before the first load.

    The audit log is the watermark store, so a failed run never moves the watermark forward. If business_key
    is given and the target holds no rows for it (other than the unknown member), the target was recreated
    since the last load, so the watermark is ignored and the next load is a full one.
    """
    if business_key is not None and not _has_business_rows(spark, target_table, business_key):
        return None
    if not spark.catalog.tableExists(audit_table):
        return None
    rows = (
        spark.read.table(audit_table)
        .where((F.col("stage") == GOLD_STAGE) & (F.col("target_table") == target_table) & (F.col("status") == "success"))
        .select(F.get_json_object("metrics", f"$.{WATERMARK_METRIC}").alias("watermark"))
        .where(F.col("watermark").isNotNull())
        .orderBy(F.col("started_at").desc())
        .limit(1)
        .collect()
    )
    return datetime.fromisoformat(rows[0]["watermark"]) if rows else None


def incremental_rows(df: DataFrame, watermark: datetime | None, column: str = CHANGE_TS) -> DataFrame:
    """Rows changed at or after the watermark.

    Equal timestamps are reprocessed on purpose: loads are idempotent, so the extra rows are harmless and a
    late write with the same timestamp is not missed.
    """
    if watermark is None:
        return df
    return df.where(F.col(column) >= F.lit(watermark))


def next_watermark(previous: datetime | None, processed: DataFrame, column: str = CHANGE_TS) -> datetime | None:
    latest = processed.agg(F.max(column)).first()[0]
    if latest is None:
        return previous
    return latest if previous is None else max(previous, latest)


def increment_with_repairs(
    source: DataFrame,
    watermark: datetime | None,
    target: DataFrame,
    business_key: str,
    key_columns: Sequence[str],
) -> DataFrame:
    """The increment, plus source rows whose target row still points at the unknown member.

    A fact loaded before its dimension existed got the unknown key. Once the dimension arrives, its row is
    unchanged, so the watermark alone would never revisit it. Rows are unioned by business key, so each appears once.
    """
    late = source.join(keys_with_unknown_member(target, business_key, key_columns), on=business_key, how="left_semi")
    return incremental_rows(source, watermark).unionByName(late).dropDuplicates([business_key])


def _has_business_rows(spark: SparkSession, target_table: str, business_key: str) -> bool:
    rows = spark.read.table(target_table).where(F.col(business_key) != "-1")
    return not rows.limit(1).isEmpty()
