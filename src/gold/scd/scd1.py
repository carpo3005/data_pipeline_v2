from pyspark.sql import DataFrame, SparkSession

from gold.merge import UpsertResult, merge_upsert

HASH = "attribute_hash"


def merge_scd1(spark: SparkSession, target_fqn: str, incoming: DataFrame, *, business_key: str) -> UpsertResult:
    """Type 1: overwrite the row in place when its attribute hash changes. No history is kept."""
    return merge_upsert(spark, target_fqn, incoming, business_key=business_key, changed_columns=(HASH,))
