from pyspark.sql import functions as F

from silver.standardisation.dates import parse_date

FORMATS = ("yyyy-MM-dd", "dd-MM-yyyy", "yyyy/MM/dd", "dd/MM/yyyy")


def test_each_format_is_parsed_and_unknown_is_null(spark):
    df = spark.createDataFrame(
        [("1990-02-03",), ("03-02-1990",), ("1990/02/03",), ("03/02/1990",), ("third of feb",), (None,)],
        "value string",
    )
    out = df.select(parse_date(F.col("value"), FORMATS).alias("d"))
    expected = ["1990-02-03"] * 4 + [None, None]
    assert [str(r[0]) if r[0] else None for r in out.collect()] == expected


def test_empty_formats_are_rejected():
    import pytest
    with pytest.raises(ValueError, match="must not be empty"):
        parse_date(F.col("value"), ())
