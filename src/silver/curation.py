from collections.abc import Sequence
from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, Window, functions as F

# Bronze metadata columns carried into silver lineage (see pipeline.metadata.MetadataColumns)
BRONZE_SOURCE_FILE = "_source_file"
BRONZE_RUN_ID = "_run_id"
BRONZE_INGESTED_AT = "_ingested_at"

# Newest first: a later version of a record wins over an earlier one
RECENCY_COLUMNS: tuple[str, ...] = ("updated_at", "created_at", BRONZE_INGESTED_AT, "source_record_id")


def source_columns(df: DataFrame) -> list[str]:
    """Business columns: everything that is not pipeline metadata (metadata starts with an underscore)."""
    return [c for c in df.columns if not c.startswith("_")]


def add_lineage_columns(
    df: DataFrame,
    *,
    source_system: str,
    pipeline_run_id: str,
) -> DataFrame:
    """Add silver lineage columns. Call before standardising so the raw payload keeps the original values."""
    return df.withColumns({
        "_raw_record_payload": F.to_json(F.struct(*[F.col(c) for c in source_columns(df)])),
        "_source_system": F.lit(source_system),
        "_source_file_path": F.col(BRONZE_SOURCE_FILE),
        "_source_file_name": F.element_at(F.split(F.col(BRONZE_SOURCE_FILE), "/"), -1),
        "_source_batch_id": F.col(BRONZE_RUN_ID),
        "_pipeline_run_id": F.lit(pipeline_run_id),
        "_ingested_timestamp": F.current_timestamp(),
    })


def deduplicate(
    df: DataFrame,
    business_key: Sequence[str],
    hash_column: str = "_record_hash",
) -> tuple[DataFrame, dict[str, int]]:
    """Drop null-key rows, exact duplicates (same hash), then keep the most recent row per business key.

    Returns the deduplicated DataFrame and the removal counts for the audit run.
    """
    has_null_key = reduce(or_, [F.col(k).isNull() for k in business_key])
    keyed = df.filter(~has_null_key)
    null_key_removed = df.count() - keyed.count()

    unique_rows = keyed.dropDuplicates([hash_column])
    duplicate_removed = keyed.count() - unique_rows.count()

    order_by = [c for c in RECENCY_COLUMNS if c in df.columns]
    window = Window.partitionBy(*business_key).orderBy(*[F.col(c).desc_nulls_last() for c in order_by])
    latest = (
        unique_rows
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    superseded_removed = unique_rows.count() - latest.count()

    counts = {
        "null_business_key_removed_count": null_key_removed,
        "duplicate_hash_removed_count": duplicate_removed,
        "superseded_removed_count": superseded_removed,
    }
    return latest, counts
