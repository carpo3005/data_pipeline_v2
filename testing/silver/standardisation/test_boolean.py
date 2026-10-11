import pytest

from pyspark.sql import Column as Col, functions as F

from silver.standardisation.boolean import assert_disjoint, normalise_boolean
from chispa.column_comparer import assert_column_equality


def test_assert_disjoint_raises_on_overlap():
    boolean_true = ("yes", "true", "1", "test")
    boolean_false = ("no", "false", "0", "test")
    with pytest.raises(ValueError, match="true_values and false_values overlap: \\['TEST'\\]"):
        assert_disjoint(boolean_true, boolean_false)

def test_assert_disjoint_passes_on_no_overlap():
    boolean_true = ("yes", "true", "1")
    boolean_false = ("no", "false", "0")
    # Should not raise any exception
    assert_disjoint(boolean_true, boolean_false)

def test_normalise_boolean(spark):
    BOOLEAN_TRUE:  tuple[str, ...] = ("Y", "YES", "TRUE", "T", "1")
    BOOLEAN_FALSE: tuple[str, ...] = ("N", "NO", "FALSE", "F", "0")

    sample_df = spark.createDataFrame(
        [
            ("Y", True),
            ("N", False),
            ("YES", True),
            ("NO", False),
            ("TRUE", True),
            ("FALSE", False),
            ("T", True),
            ("F", False),
            ("1", True),
            ("0", False),
            (" yes ", True),   # trimmed and case-insensitive
            ("maybe", None),   # unrecognised values become NULL
        ],
        "value string, expected boolean",
    )

    result_df = sample_df.withColumn("actual", normalise_boolean(F.col("value"), BOOLEAN_TRUE, BOOLEAN_FALSE))

    assert_column_equality(result_df, "actual", "expected")
