from pipeline.catalog import ensure_schema_exists, get_schema_fqn, get_table_fqn
from pipeline.writer import replace_table

CATALOG = "forge"
TEST_SCHEMA = "test_replace_table"


def test_replace_table_updates_metastore_columns_after_schema_change(spark):
    ensure_schema_exists(spark, CATALOG, TEST_SCHEMA)
    table = get_table_fqn(CATALOG, TEST_SCHEMA, "address")
    try:
        replace_table(spark.createDataFrame([("1",)], "address_id string"), table)
        replace_table(spark.createDataFrame([("1", "x")], "address_id string, _run_id string"), table)

        assert [c.name for c in spark.catalog.listColumns(table)] == ["address_id", "_run_id"]
        assert spark.read.table(table).count() == 1
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {get_schema_fqn(CATALOG, TEST_SCHEMA)} CASCADE")
