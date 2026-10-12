from datetime import date, datetime, timedelta

from silver.data_quality.checks import CHECKS
from silver.data_quality.raw_copies import keep_raw_copies
from silver.data_quality.rules import Rule

NULL_TOKENS = ("", "NULL", "N/A")


def _passes(df, rule: Rule) -> list[bool]:
    return [row[0] for row in df.select(rule.passes().alias("p")).collect()]


def test_not_null_fails_on_null_only(isolated_spark):
    df = isolated_spark.createDataFrame([("a",), (None,)], "value string")
    assert _passes(df, Rule("T-001", "H", "not_null", ("value",))) == [True, False]


def test_matches_regex_and_in_set_pass_on_null(isolated_spark):
    df = isolated_spark.createDataFrame([("CAT-0001",), ("bad",), (None,)], "code string")
    assert _passes(df, Rule("T-001", "S", "matches_regex", ("code",), {"pattern": r"^CAT-[0-9]{4}$"})) == [
        True, False, True,
    ]
    assert _passes(df, Rule("T-002", "S", "in_set", ("code",), {"values": ("CAT-0001",)})) == [True, False, True]


def test_in_range_applies_minimum_and_maximum(isolated_spark):
    df = isolated_spark.createDataFrame([(-1,), (0,), (5,), (11,), (None,)], "amount int")
    rule = Rule("T-001", "H", "in_range", ("amount",), {"minimum": 0, "maximum": 10})
    assert _passes(df, rule) == [False, True, True, False, True]


def test_not_future_allows_past_and_rejects_future(isolated_spark):
    past = date(2000, 1, 1)
    future = datetime.now() + timedelta(days=2)
    df = isolated_spark.createDataFrame([(past,), (future,), (None,)], "value date")
    assert _passes(df, Rule("T-001", "H", "not_future", ("value",))) == [True, False, True]


def test_column_lte_column_and_not_equal_columns(isolated_spark):
    df = isolated_spark.createDataFrame([(1, 2), (3, 3), (5, 4), (None, 1)], "a int, b int")
    assert _passes(df, Rule("T-001", "S", "column_lte_column", ("a", "b"))) == [True, True, False, True]
    assert _passes(df, Rule("T-002", "S", "not_equal_columns", ("a", "b"))) == [True, False, True, True]


def test_sql_expression_with_tolerance_and_null_passes(isolated_spark):
    df = isolated_spark.createDataFrame([(10.0, 2.0, 8.0), (10.0, 2.0, 9.0), (None, 2.0, 8.0)], "a double, b double, c double")
    rule = Rule("T-001", "S", "sql_expression", ("a", "b", "c"), {"expression": "abs(a - b - c) <= 0.01"})
    assert _passes(df, rule) == [True, False, True]


def test_parsed_fails_only_when_original_present_and_standardised_null(isolated_spark):
    # standardised value is NULL where the original was present but could not be parsed
    df = isolated_spark.createDataFrame(
        [("2000-01-01", date(2000, 1, 1)), ("31/31/2000", None), (None, None)],
        "date_of_birth_raw string, date_of_birth date",
    )
    rule = Rule("T-001", "S", "parsed", ("date_of_birth",))
    assert _passes(df, rule) == [True, False, True]


def test_keep_raw_copies_trims_and_nulls_tokens(isolated_spark):
    df = isolated_spark.createDataFrame([("  Scotland ",), ("N/A",), (" ",)], "region string")
    out = keep_raw_copies(df, ("region",), NULL_TOKENS)
    assert [r[0] for r in out.select("region_raw").collect()] == ["Scotland", None, None]


def test_every_check_is_registered():
    assert set(CHECKS) == {
        "not_null", "parsed", "matches_regex", "in_set", "in_range", "not_future",
        "column_lte_column", "not_equal_columns", "sql_expression",
    }
