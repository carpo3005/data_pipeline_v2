from collections.abc import Mapping, Sequence
from functools import reduce
from operator import add

from pyspark.sql import Column, DataFrame, functions as F

from gold.keys import UNKNOWN_KEY


def _col(value: str | Column) -> Column:
    return F.col(value) if isinstance(value, str) else value


def point_in_time_key(
    facts: DataFrame,
    dimension: DataFrame,
    *,
    fact_key: str | Column,
    dim_key: str,
    fact_time: str | Column,
    dim_sk: str,
    output: str,
    start_column: str = "effective_start_date",
    end_column: str = "effective_end_date",
    unknown: int = UNKNOWN_KEY,
) -> DataFrame:
    """Add the surrogate key of the SCD2 version that was valid at each fact's time.

    A version covers [start, end): a fact at exactly start gets the new version. No version gives `unknown`.
    Versions for one key must not overlap, or the fact row is duplicated.
    """
    versions = dimension.select(
        F.col(dim_key).alias("_v_key"),
        F.col(dim_sk).alias("_v_sk"),
        F.col(start_column).alias("_v_start"),
        F.col(end_column).alias("_v_end"),
    )
    time = _col(fact_time)
    on = (_col(fact_key) == F.col("_v_key")) & (time >= F.col("_v_start")) & (time < F.col("_v_end"))
    return (
        facts.join(versions, on, "left")
        .withColumn(output, F.coalesce(F.col("_v_sk"), F.lit(unknown)))
        .drop("_v_key", "_v_sk", "_v_start", "_v_end")
    )


def key_lookup(
    facts: DataFrame,
    dimension: DataFrame,
    *,
    fact_key: str | Column,
    dim_key: str,
    dim_sk: str,
    output: str,
    unknown: int = UNKNOWN_KEY,
) -> DataFrame:
    """Add the surrogate key of a dimension that has one row per key (SCD1, dim_date)."""
    keys = dimension.select(F.col(dim_key).alias("_v_key"), F.col(dim_sk).alias("_v_sk"))
    return (
        facts.join(keys, _col(fact_key) == F.col("_v_key"), "left")
        .withColumn(output, F.coalesce(F.col("_v_sk"), F.lit(unknown)))
        .drop("_v_key", "_v_sk")
    )


def count_unknown(df: DataFrame, columns: Sequence[str], unknown: int = UNKNOWN_KEY) -> dict[str, int]:
    """Rows that fell back to the unknown member, per key column. Keys: unknown_<column>_count."""
    if not columns:
        return {}
    aggregates = [F.sum(F.when(F.col(c) == unknown, 1).otherwise(0)).alias(f"u{i}") for i, c in enumerate(columns)]
    row = df.agg(*aggregates).first()
    return {f"unknown_{c}_count": int(row[f"u{i}"] or 0) for i, c in enumerate(columns)}


def keys_with_unknown_member(
    target: DataFrame,
    business_key: str,
    key_columns: Sequence[str],
    unknown: int = UNKNOWN_KEY,
) -> DataFrame:
    """Business keys of fact rows that still point at the unknown member, so a later load can fix them
    once the dimension row arrives (late-arriving dimension)."""
    if not key_columns:
        return target.select(business_key).limit(0)
    any_unknown = reduce(add, [F.when(F.col(c) == unknown, 1).otherwise(0) for c in key_columns])
    return target.where(any_unknown > 0).select(business_key).distinct()
