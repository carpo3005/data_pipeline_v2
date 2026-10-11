from pyspark.sql import functions as F

from silver.standardisation.phone import normalise_phone


def test_uk_phone_formats_normalise_to_e164(spark):
    df = spark.createDataFrame(
        [("07700 900123",), ("+44 7700 900123",), ("(0113) 555 0199",), ("447700900123",)],
        "phone string",
    )
    out = df.select(normalise_phone(F.col("phone"), "44", "0", (10,)).alias("p"))
    assert [r[0] for r in out.collect()] == ["+447700900123", "+447700900123", "+441135550199", "+447700900123"]


def test_wrong_length_or_garbage_is_null(spark):
    df = spark.createDataFrame([("0770",), ("not a number",), (None,)], "phone string")
    out = df.select(normalise_phone(F.col("phone"), "44", "0", (10,)).alias("p"))
    assert [r[0] for r in out.collect()] == [None, None, None]
