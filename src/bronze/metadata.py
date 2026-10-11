from pyspark.sql import DataFrame, functions as F

def add_record_hash(df: DataFrame) -> DataFrame:
    """Add a deterministic hash of the raw source columns."""
    row_json = F.to_json(
        F.struct(*[F.col(name) for name in df.columns ]),
        options={"ignoreNullFields": "false"},
    )

    return (df
        .withColumn(
            "_record_hash",
            F.sha2(row_json, 256)
        ) 
    )

METADATA_COLUMNS = ("_record_hash", "_run_id", "_source_file", "_ingested_at")

def add_metadata(df: DataFrame, *, run_id: str, ingested_at: str, source_columns: list[str]) -> DataFrame:
    df = add_record_hash(df)   # hash first, before other metadata exists
    return (df
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.lit(ingested_at))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )