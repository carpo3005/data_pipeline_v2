import pytest
from silver.standardisation.string_cleaning import (
    clean_string, trim_whitespace, nullify_tokens, apply_case
)

from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StringType, StructType, StructField
from chispa.dataframe_comparer import assert_df_equality

NULL_TOKENS: tuple[str, ...] = ("", "NULL", "null", "N/A", "NA", "None")

@pytest.fixture
def messy_columns(spark) -> DataFrame:
    from pyspark.sql import Row
    return spark.createDataFrame([
        ("  foo  ", "foo"),
        ("bar\tbaz", "bar baz"),
        ("   qux  quux   ", "qux quux"),
    ],
        ["messy", "value"]
    )

def test_trim_whitespace(messy_columns):
    from pyspark.sql import functions as F
    trimmed = messy_columns.withColumn("trimmed", trim_whitespace(F.col("messy"))).select("trimmed")
    actual_df = trimmed
    expected_df = messy_columns.select("value").withColumnRenamed("value", "trimmed")
    assert_df_equality(actual_df, expected_df)

@pytest.fixture
def columns_with_nulls(spark) -> DataFrame:

    schema = StructType([StructField("null_token", StringType(), True), StructField("value", StringType(), True)])
    return spark.createDataFrame([
            ("", None),
            ("NULL", None),
            ("null", None),
            ("N/A", None),
            ("NA", None),
            ("None", None),
        ],
            schema=schema
        )

def test_nullify_tokens(columns_with_nulls):
    from pyspark.sql import functions as F
    nullified = columns_with_nulls.withColumn("nullified", nullify_tokens(F.col("null_token"), NULL_TOKENS)).select("nullified")
    actual_df = nullified
    expected_df = columns_with_nulls.select("value").withColumnRenamed("value", "nullified")
    assert_df_equality(actual_df, expected_df)


@pytest.fixture
def mixed_case_column(spark) -> DataFrame:
    return spark.createDataFrame([("mIxEd CaSe",), ("hello world",), (None,)], ["raw"])


@pytest.mark.parametrize("case, expected", [
    ("upper", ["MIXED CASE", "HELLO WORLD", None]),
    ("lower", ["mixed case", "hello world", None]),
    ("title", ["Mixed Case", "Hello World", None]),
])
def test_apply_case(spark, mixed_case_column, case, expected):
    from pyspark.sql import functions as F
    actual_df = mixed_case_column.select(apply_case(F.col("raw"), case).alias("cased"))
    expected_df = spark.createDataFrame([(v,) for v in expected], ["cased"])
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_case_none_leaves_column_unchanged(spark, mixed_case_column):
    from pyspark.sql import functions as F
    actual_df = mixed_case_column.select(apply_case(F.col("raw"), None).alias("cased"))
    expected_df = mixed_case_column.select(F.col("raw").alias("cased"))
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_case_rejects_unknown_case(mixed_case_column):
    from pyspark.sql import functions as F
    with pytest.raises(ValueError, match="Unknown case: 'sentence'"):
        apply_case(F.col("raw"), "sentence")
