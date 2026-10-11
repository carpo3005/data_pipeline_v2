from collections.abc import Iterable

from pyspark.sql import DataFrame, functions as F

from pipeline.validation import assert_columns_exist
from silver.contract import CastRule
from silver.standardisation.result import StandardisationResult, count_invalid
from silver.standardisation.string_cleaning import clean_string


def cast_columns(
    df: DataFrame,
    cast_rules: Iterable[CastRule],
    null_tokens: tuple[str, ...],
    count_invalid_values: bool = True,
) -> StandardisationResult:
    """Trim, turn null tokens into NULL, then try_cast each column (bad values become NULL)."""
    rules = tuple(cast_rules)
    assert_columns_exist(df, [rule.column_name for rule in rules])

    outputs = {}
    checks = {}
    for rule in rules:
        cleaned = clean_string(F.col(rule.column_name), null_tokens)
        outputs[rule.column_name] = cleaned.try_cast(rule.data_type)
        checks[rule.column_name] = (cleaned, outputs[rule.column_name])

    counts = count_invalid(df, checks) if count_invalid_values else {}
    return StandardisationResult(df=df.withColumns(outputs), invalid_counts=counts)
