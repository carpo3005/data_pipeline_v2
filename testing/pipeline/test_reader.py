import pytest
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pipeline.catalog import ensure_schema_exists, get_table_fqn

from chispa.dataframe_comparer import assert_df_equality

from pipeline.reader import read_source_csv, read_table
from pipeline.writer import create_delta_table

COLUMNS = [
    "source_record_id", "address_id", "customer_id",
]
SCHEMA = StructType([StructField(c, StringType(), True) for c in COLUMNS])


def test_read_source_csv(spark: SparkSession, tmp_path):
    csv_file = tmp_path / "address_B001.csv"
    csv_file.write_text(
        ",".join(COLUMNS) + "\n"
        "SRC_ADDRESS_B001_000000001,A00000001,C0000001"
    )

    df = read_source_csv(spark, SCHEMA, str(csv_file))

    expected = [
        Row(
            source_record_id="SRC_ADDRESS_B001_000000001",
            address_id="A00000001",
            customer_id="C0000001",
        )
    ]

    assert df.columns == COLUMNS
    assert df.collect() == expected

def test_read_table(spark: SparkSession, sample_df: DataFrame):
    schema = "bronze"
    catalog = "forge"
    table_fqn = get_table_fqn(catalog, schema, "test_read_table")
    ensure_schema_exists(spark, catalog, schema)

    # sample_df.write.format("delta").mode("overwrite").saveAsTable(table_fqn)
    create_delta_table(sample_df, table_fqn)
    try:
        df = read_table(spark, table_fqn)
        assert_df_equality(df, sample_df,ignore_row_order=True)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")