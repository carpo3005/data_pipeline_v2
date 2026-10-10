from pipeline.writer import create_delta_table,append_table
from pipeline.catalog import (
    get_table_fqn,
    ensure_schema_exists,
)

from pyspark.sql import SparkSession

from chispa.dataframe_comparer import assert_df_equality

def test_create_delta_table(spark: SparkSession, sample_df):
    catalog = "forge"
    schema = "bronze"
    table_fqn = get_table_fqn(catalog, schema, "test_create_delta_table")
    ensure_schema_exists(spark, catalog, schema)

    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        create_delta_table(sample_df, table_fqn)
        df = spark.table(table_fqn)
        assert_df_equality(df, sample_df, ignore_row_order=True)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")

def test_append_table(spark: SparkSession, sample_df):
    catalog = "forge"
    schema = "bronze"

    table_fqn = get_table_fqn(catalog, schema, "test_append_table")
    ensure_schema_exists(spark, catalog, schema)

    try:
        create_delta_table(sample_df, table_fqn)
        append_table(sample_df, table_fqn)
        df = spark.table(table_fqn)
        expected_df = sample_df.union(sample_df)
        assert_df_equality(df, expected_df, ignore_row_order=True)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")

