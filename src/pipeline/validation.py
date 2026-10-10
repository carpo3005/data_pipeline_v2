from pyspark.sql import SparkSession, DataFrame
from typing import Iterable
from functools import reduce
from operator import or_

def missing_columns(spark: SparkSession, df: DataFrame, required_columns: Iterable[str]) -> list[str]:
    return [col for col in required_columns if col not in df.columns]

def assert_columns_exist(spark: SparkSession, df: DataFrame, required_columns: Iterable[str]) -> None:
    missing = missing_columns(spark, df, required_columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")

def any_key_null(df: DataFrame, key_columns: Iterable[str]) -> bool:
    return any(df.filter(df[col].isNull()).count() > 0 for col in key_columns)

def count_null_business_keys(df: DataFrame, key_columns: Iterable[str]) -> int:
    condition = reduce(or_, (df[col].isNull() for col in key_columns), False)
    return df.filter(condition).count()