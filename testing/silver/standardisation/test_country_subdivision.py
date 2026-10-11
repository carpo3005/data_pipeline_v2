from pyspark.sql import functions as F

from silver.standardisation.country import normalise_country
from silver.standardisation.subdivision import map_uk_subdivision

COUNTRY_ALIASES = {"UK": "GB", "England": "GB", "great britain": "GB", "GB": "GB"}
SUBDIVISIONS = {"Greater London": "England", "wales": "Wales"}


def _values(df, col):
    return [r[0] for r in df.select(col).collect()]


def test_country_aliases_are_mapped_case_insensitively(spark):
    df = spark.createDataFrame([("uk",), (" Great  Britain ",), ("GB",), ("France",), (None,)], "country string")
    out = df.select(normalise_country(F.col("country"), COUNTRY_ALIASES).alias("c"))
    assert _values(out, "c") == ["GB", "GB", "GB", None, None]


def test_empty_country_aliases_give_null(spark):
    df = spark.createDataFrame([("GB",)], "country string")
    assert _values(df.select(normalise_country(F.col("country"), {}).alias("c")), "c") == [None]


def test_subdivision_mapped_to_nation(spark):
    df = spark.createDataFrame([("greater london",), ("WALES",), ("Kent",), (None,)], "region string")
    out = df.select(map_uk_subdivision(F.col("region"), SUBDIVISIONS).alias("n"))
    assert _values(out, "n") == ["England", "Wales", None, None]
