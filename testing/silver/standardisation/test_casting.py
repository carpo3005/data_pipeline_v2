from decimal import Decimal

from silver.contract import CastRule
from silver.standardisation.casting import cast_columns

NULL_TOKENS = ("", "NULL", "N/A")


def _rows(df, *columns):
    return [tuple(r) for r in df.select(*columns).collect()]


def test_casts_to_declared_types(spark):
    df = spark.createDataFrame([("12.50", "7")], "amount string, quantity string")
    rules = (CastRule("amount", "DECIMAL(18,2)"), CastRule("quantity", "INT"))
    result = cast_columns(df, rules, NULL_TOKENS)
    assert _rows(result.df, "amount", "quantity") == [(Decimal("12.50"), 7)]
    assert result.invalid_counts == {"invalid_value_count": 0}


def test_unparseable_values_become_null_and_are_counted(spark):
    df = spark.createDataFrame([("abc",), ("3",), ("N/A",), (None,)], "quantity string")
    result = cast_columns(df, (CastRule("quantity", "INT"),), NULL_TOKENS)
    # ANSI mode would raise on "abc"; try_cast returns NULL. "N/A" is a null token, so not counted.
    assert _rows(result.df, "quantity") == [(None,), (3,), (None,), (None,)]
    assert result.invalid_counts == {"invalid_quantity_count": 1, "invalid_value_count": 1}


def test_cast_can_be_skipped_for_counting(spark):
    df = spark.createDataFrame([("abc",)], "quantity string")
    result = cast_columns(df, (CastRule("quantity", "INT"),), NULL_TOKENS, count_invalid_values=False)
    assert result.invalid_counts == {}
