import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from pipeline.errors import DataValidationError
from pipeline.validation import (
    missing_columns,
    assert_columns_exist,
    assert_schema_matches,
    any_key_null,
    count_null_business_keys,
    count_duplicate_keys,
)


# --------------------------------------------------------------------------------
# Column checks
# --------------------------------------------------------------------------------


def test_missing_columns_returns_absent_columns_in_order(sample_df):
    result = missing_columns(sample_df, ["age", "email", "phone"])
    assert result == ["email", "phone"]

def test_missing_columns_empty_when_all_present(sample_df):
    assert missing_columns(sample_df, ["first_name", "age"]) == []

def test_assert_columns_exist_passes_when_present(sample_df):
    assert_columns_exist(sample_df, ["first_name", "last_name"])

def test_assert_columns_exist_raises_listing_missing(sample_df):
    with pytest.raises(DataValidationError, match="email, phone"):
        assert_columns_exist(sample_df, ["first_name", "email", "phone"])


# --------------------------------------------------------------------------------
# Schema checks
# --------------------------------------------------------------------------------


def _schema(*names: str) -> StructType:
    return StructType([StructField(n, StringType()) for n in names])

def test_assert_schema_matches_passes_on_exact_match(sample_df):
    assert_schema_matches(sample_df, _schema("first_name", "last_name", "age"))

def test_assert_schema_matches_reports_missing_columns(sample_df):
    with pytest.raises(DataValidationError, match="missing columns: email"):
        assert_schema_matches(sample_df, _schema("first_name", "last_name", "age", "email"))

def test_assert_schema_matches_reports_unexpected_columns(sample_df):
    with pytest.raises(DataValidationError, match="unexpected columns: age"):
        assert_schema_matches(sample_df, _schema("first_name", "last_name"))

def test_assert_schema_matches_allow_extra(sample_df):
    assert_schema_matches(sample_df, _schema("first_name", "last_name"), allow_extra=True)

def test_assert_schema_matches_ignores_named_columns(sample_df):
    assert_schema_matches(sample_df, _schema("first_name", "last_name"), ignore=["age"])


# --------------------------------------------------------------------------------
# Key checks
# --------------------------------------------------------------------------------


def test_any_key_null_true_when_key_is_null(sample_df):
    # sample_df has a null last_name in the "Dan" row
    assert any_key_null(sample_df, ["last_name"]) is True

def test_any_key_null_false_when_keys_are_complete(sample_df):
    assert any_key_null(sample_df, ["first_name", "age"]) is False

def test_count_null_business_keys_counts_rows(sample_df):
    # Only one row has a null last_name
    assert count_null_business_keys(sample_df, ["last_name"]) == 1

def test_count_null_business_keys_zero_when_complete(sample_df):
    assert count_null_business_keys(sample_df, ["first_name", "age"]) == 0

def test_count_null_business_keys_counts_row_once_when_several_keys_null(spark: SparkSession):
    df = spark.createDataFrame(
        [("A", None, None), ("B", None, "x"), ("C", "y", "z")],
        ["id", "k1", "k2"],
    )
    # Row A has two null keys but is one bad row, so the count should be 2 (rows A and B)
    assert count_null_business_keys(df, ["k1", "k2"]) == 2

def test_count_duplicate_keys_zero_when_unique(sample_df):
    assert count_duplicate_keys(sample_df, ["first_name"]) == 0

def test_count_duplicate_keys_counts_rows_beyond_first(sample_df):
    # age 30 appears three times: two rows beyond the first
    assert count_duplicate_keys(sample_df, ["age"]) == 2
