import pytest
from pyspark.sql import SparkSession

from pipeline.validation import (
    missing_columns,
    assert_columns_exist,
    any_key_null,
    count_null_business_keys,
)


# --------------------------------------------------------------------------------
# Column checks
# --------------------------------------------------------------------------------


def test_missing_columns_returns_absent_columns_in_order(spark: SparkSession, sample_df):
    result = missing_columns(spark, sample_df, ["age", "email", "phone"])
    assert result == ["email", "phone"]

def test_missing_columns_empty_when_all_present(spark: SparkSession, sample_df):
    assert missing_columns(spark, sample_df, ["first_name", "age"]) == []

def test_assert_columns_exist_passes_when_present(spark: SparkSession, sample_df):
    assert_columns_exist(spark, sample_df, ["first_name", "last_name"])

def test_assert_columns_exist_raises_listing_missing(spark: SparkSession, sample_df):
    with pytest.raises(ValueError, match="email, phone"):
        assert_columns_exist(spark, sample_df, ["first_name", "email", "phone"])


# --------------------------------------------------------------------------------
# Key checks
# --------------------------------------------------------------------------------


def test_any_key_null_true_when_key_is_null(spark: SparkSession, sample_df):
    # sample_df has a null last_name in the "Dan" row
    assert any_key_null(sample_df, ["last_name"]) is True

def test_any_key_null_false_when_keys_are_complete(spark: SparkSession, sample_df):
    assert any_key_null(sample_df, ["first_name", "age"]) is False

def test_any_key_null_ignores_nulls_outside_key_columns(spark: SparkSession, sample_df):
    # last_name is null for one row, but it isn't a key here
    assert any_key_null(sample_df, ["first_name", "age"]) is False

def test_count_null_business_keys_counts_rows(spark: SparkSession, sample_df):
    # Only one row has a null last_name
    assert count_null_business_keys(sample_df, ["last_name"]) == 1

def test_count_null_business_keys_zero_when_complete(spark: SparkSession, sample_df):
    assert count_null_business_keys(sample_df, ["first_name", "age"]) == 0

def test_count_null_business_keys_counts_row_once_when_several_keys_null(spark: SparkSession):
    df = spark.createDataFrame(
        [("A", None, None), ("B", None, "x"), ("C", "y", "z")],
        ["id", "k1", "k2"],
    )
    # Row A has two null keys but is one bad row, so the count should be 2 (rows A and B)
    assert count_null_business_keys(df, ["k1", "k2"]) == 2
