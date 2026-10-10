import pytest
from pyspark.sql import SparkSession
from pipeline.session import is_databricks
from pipeline.catalog import (
    check_catalog_exists,
    check_schema_exists,
    check_table_exists,

    ensure_schema_exists,
    ensure_catalog_exists,

    assert_catalog_exists,
    assert_tables_exist,
    assert_schema_exists,

    get_table_fqn,
    get_schema_fqn,
)

CATALOG = "forge"
TEST_SCHEMA = "test_catalog"

requires_databricks = pytest.mark.skipif(
    not is_databricks(), reason="Requires Unity Catalog (Databricks)"
)


@pytest.fixture
def test_schema(spark: SparkSession):
    """Create a throwaway schema for a test and drop it (and everything in it) afterwards."""
    ensure_schema_exists(spark, CATALOG, TEST_SCHEMA)
    yield TEST_SCHEMA
    spark.sql(f"DROP SCHEMA IF EXISTS {get_schema_fqn(CATALOG, TEST_SCHEMA)} CASCADE")


# --------------------------------------------------------------------------------
# Getters
# --------------------------------------------------------------------------------


def test_get_schema_fqn_local(monkeypatch):
    monkeypatch.setattr("pipeline.catalog.is_databricks", lambda: False)
    assert get_schema_fqn(CATALOG, "bronze") == "bronze"

def test_get_schema_fqn_databricks(monkeypatch):
    monkeypatch.setattr("pipeline.catalog.is_databricks", lambda: True)
    assert get_schema_fqn(CATALOG, "bronze") == "forge.bronze"

def test_get_table_fqn_local(monkeypatch):
    monkeypatch.setattr("pipeline.catalog.is_databricks", lambda: False)
    assert get_table_fqn(CATALOG, "bronze", "customer") == "bronze.customer"

def test_get_table_fqn_databricks(monkeypatch):
    monkeypatch.setattr("pipeline.catalog.is_databricks", lambda: True)
    assert get_table_fqn(CATALOG, "bronze", "customer") == "forge.bronze.customer"


# --------------------------------------------------------------------------------
# Ensurers
# --------------------------------------------------------------------------------


def test_ensure_schema_exists_creates_and_is_idempotent(spark: SparkSession):
    schema = "test_catalog_ensure"
    try:
        ensure_schema_exists(spark, CATALOG, schema)
        assert check_schema_exists(spark, CATALOG, schema)
        # Second call must not raise
        ensure_schema_exists(spark, CATALOG, schema)
        assert check_schema_exists(spark, CATALOG, schema)
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {get_schema_fqn(CATALOG, schema)} CASCADE")

@requires_databricks
def test_ensure_catalog_exists_creates_and_is_idempotent(spark: SparkSession):
    catalog = "test_catalog_ensure"
    try:
        ensure_catalog_exists(spark, catalog)
        assert check_catalog_exists(spark, catalog)
        ensure_catalog_exists(spark, catalog)
        assert check_catalog_exists(spark, catalog)
    finally:
        spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")


# --------------------------------------------------------------------------------
# Checkers
# --------------------------------------------------------------------------------


def test_check_table_exists(spark: SparkSession, sample_df, test_schema):
    present = get_table_fqn(CATALOG, test_schema, "present")
    missing = get_table_fqn(CATALOG, test_schema, "missing")
    sample_df.write.format("delta").saveAsTable(present)

    assert check_table_exists(spark, present)
    assert not check_table_exists(spark, missing)

def test_check_schema_exists(spark: SparkSession, test_schema):
    assert check_schema_exists(spark, CATALOG, test_schema)
    assert not check_schema_exists(spark, CATALOG, "no_such_schema_xyz")

@requires_databricks
def test_check_catalog_exists(spark: SparkSession):
    assert check_catalog_exists(spark, "hive_metastore")
    assert not check_catalog_exists(spark, "no_such_catalog_xyz")

@pytest.mark.skipif(is_databricks(), reason="Only raises when not on Databricks")
def test_check_catalog_exists_raises_locally(spark: SparkSession):
    with pytest.raises(NotImplementedError):
        check_catalog_exists(spark, CATALOG)


# --------------------------------------------------------------------------------
# Asserters
# --------------------------------------------------------------------------------


def test_assert_tables_exist_passes_when_present(spark: SparkSession, sample_df, test_schema):
    sample_df.write.format("delta").saveAsTable(get_table_fqn(CATALOG, test_schema, "present"))

    assert_tables_exist(spark, CATALOG, test_schema, ["present"])

def test_assert_tables_exist_raises_on_missing(spark: SparkSession, sample_df, test_schema):
    sample_df.write.format("delta").saveAsTable(get_table_fqn(CATALOG, test_schema, "present"))

    with pytest.raises(ValueError, match="missing"):
        assert_tables_exist(spark, CATALOG, test_schema, ["present", "missing"])

def test_assert_schema_exists_passes_when_present(spark: SparkSession, test_schema):
    assert_schema_exists(spark, CATALOG, test_schema)

def test_assert_schema_exists_raises_on_missing(spark: SparkSession):
    with pytest.raises(ValueError, match="Missing schema"):
        assert_schema_exists(spark, CATALOG, "no_such_schema_xyz")

@requires_databricks
def test_assert_catalog_exists_passes_when_present(spark: SparkSession):
    assert_catalog_exists(spark, "hive_metastore")

@requires_databricks
def test_assert_catalog_exists_raises_on_missing(spark: SparkSession):
    with pytest.raises(ValueError, match="Missing catalog"):
        assert_catalog_exists(spark, "no_such_catalog_xyz")
