from functools import reduce
from operator import or_
from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from pipeline.errors import DataValidationError


def missing_columns(df: DataFrame, required_columns: Iterable[str]) -> list[str]:
    return [col for col in required_columns if col not in df.columns]

def assert_columns_exist(df: DataFrame, required_columns: Iterable[str]) -> None:
    missing = missing_columns(df, required_columns)
    if missing:
        raise DataValidationError(f"Missing columns: {', '.join(missing)}")

def assert_schema_matches(
    df: DataFrame,
    expected: StructType,
    allow_extra: bool = False,
    ignore: Iterable[str] = (),
) -> None:
    """Fail if df lacks any expected column, or (unless allow_extra) has columns not in expected.

    Columns in `ignore` (e.g. metadata columns) are left out of the comparison.
    """
    ignored = set(ignore)
    expected_names = [f.name for f in expected.fields if f.name not in ignored]
    actual_names = [c for c in df.columns if c not in ignored]

    problems = []
    missing = [c for c in expected_names if c not in actual_names]
    if missing:
        problems.append(f"missing columns: {', '.join(missing)}")
    if not allow_extra:
        unexpected = [c for c in actual_names if c not in expected_names]
        if unexpected:
            problems.append(f"unexpected columns: {', '.join(unexpected)}")
    if problems:
        raise DataValidationError(f"Schema mismatch: {'; '.join(problems)}")

def any_key_null(df: DataFrame, key_columns: Iterable[str]) -> bool:
    return any(df.filter(df[col].isNull()).count() > 0 for col in key_columns)

def count_null_business_keys(df: DataFrame, key_columns: Iterable[str]) -> int:
    condition = reduce(or_, (df[col].isNull() for col in key_columns), False)
    return df.filter(condition).count()

def count_duplicate_keys(df: DataFrame, key_columns: Iterable[str]) -> int:
    """Number of rows beyond the first for each key (0 means keys are unique)."""
    keys = list(key_columns)
    return df.count() - df.dropDuplicates(keys).count()
