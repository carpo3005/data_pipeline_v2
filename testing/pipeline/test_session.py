import pytest

from pathlib import Path

from pyspark.sql import SparkSession
from src.pipeline.session import get_spark_session, is_databricks




def test_spark_setup(spark):
    assert isinstance(spark, SparkSession), "The returned object is not a SparkSession."

def test_timzone_is_UTC(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

@pytest.mark.skipif(is_databricks(), reason="Skipping this test on non-Databricks environment.")
def test_delta_lake(spark, sample_df, tmp_path):
    
    table_path = str(tmp_path / "delta_table")
    sample_df.write.format("delta").save(table_path)
    result = spark.read.format("delta").load(table_path)

    assert result.count() == sample_df.count()

    