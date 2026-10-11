from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from pyspark.sql import DataFrame, functions as F


@dataclass(frozen=True)
class MetadataColumns:
    """Names of the metadata columns the pipeline adds. Override to use different names."""
    record_hash: str = "_record_hash"
    run_id: str = "_run_id"
    source_file: str = "_source_file"
    ingested_at: str = "_ingested_at"


def add_record_hash(df: DataFrame, source_columns: Iterable[str], column: str = "_record_hash") -> DataFrame:
    """Add a deterministic SHA-256 hash of the given source columns (nulls included)."""
    row_json = F.to_json(
        F.struct(*[F.col(name) for name in source_columns]),
        options={"ignoreNullFields": "false"},
    )
    return df.withColumn(column, F.sha2(row_json, 256))


def add_metadata(
    df: DataFrame,
    *,
    run_id: str,
    ingested_at: datetime,
    source_columns: Iterable[str],
    columns: MetadataColumns = MetadataColumns(),
) -> DataFrame:
    """Add the record hash, run id and ingestion timestamp. The hash only covers source_columns."""
    df = add_record_hash(df, source_columns, columns.record_hash)
    return (
        df
        .withColumn(columns.run_id, F.lit(run_id))
        .withColumn(columns.ingested_at, F.lit(ingested_at).cast("timestamp"))
    )
