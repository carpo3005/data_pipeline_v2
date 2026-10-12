from collections.abc import Iterable

from pyspark.sql import DataFrame, functions as F

from pipeline.validation import assert_columns_exist
from silver.standardisation.string_cleaning import clean_string

RAW_SUFFIX = "_raw"


def raw_column(column: str) -> str:
    return f"{column}{RAW_SUFFIX}"


def keep_raw_copies(df: DataFrame, columns: Iterable[str], null_tokens: tuple[str, ...]) -> DataFrame:
    """Add <column>_raw holding the original value, trimmed, with null tokens as NULL.

    Call before standardising, so the parsed rules can tell "missing" from "could not parse".
    """
    names = list(columns)
    assert_columns_exist(df, names)
    return df.withColumns({raw_column(c): clean_string(F.col(c), null_tokens) for c in names})
