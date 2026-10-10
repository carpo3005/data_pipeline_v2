from pyspark.sql import SparkSession
from typing import Iterable

from pipeline.session import is_databricks

# --------------------------------------------------------------------------------
# Ensurers
# --------------------------------------------------------------------------------


def ensure_schema_exists(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create a schema (database) if it's missing."""
    schema_fqn = get_schema_fqn(catalog, schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")

def ensure_catalog_exists(spark: SparkSession, catalog_name: str) -> None:
    """Create a catalog if it's missing. Only works where Unity Catalog is available (Databricks)."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")


# --------------------------------------------------------------------------------
# Getters
# --------------------------------------------------------------------------------


def get_table_fqn(catalog: str, schema: str, table_name: str) -> str:
    return f"{get_schema_fqn(catalog, schema)}.{table_name}"

def get_schema_fqn(catalog: str, schema: str) -> str:
    if is_databricks():
        return f"{catalog}.{schema}"
    return schema


# --------------------------------------------------------------------------------
# Checkers
# --------------------------------------------------------------------------------


def check_table_exists(spark: SparkSession, table_fqn: str) -> bool:
    return spark.catalog.tableExists(table_fqn)

def check_schema_exists(spark: SparkSession, catalog: str, schema: str) -> bool:
    if is_databricks():
        return schema in [row[0] for row in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]
    return spark.catalog.databaseExists(schema)

def check_catalog_exists(spark: SparkSession, catalog: str) -> bool:
    if is_databricks():
        return catalog in [c.name for c in spark.catalog.listCatalogs()]
    raise NotImplementedError("Catalog existence check is only supported on Databricks.")


# --------------------------------------------------------------------------------
# Finders
# --------------------------------------------------------------------------------

def find_missing_tables(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_names: Iterable[str],
) -> list[str]:
    table_fqns = [get_table_fqn(catalog, schema, name) for name in table_names]
    return [fqn for fqn in table_fqns if not check_table_exists(spark, fqn)]


# --------------------------------------------------------------------------------
# Asserters
# --------------------------------------------------------------------------------


def assert_tables_exist(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_names: Iterable[str],
) -> None:
    missing = find_missing_tables(spark, catalog, schema, table_names)
    if missing:
        raise ValueError(f"Missing tables: {', '.join(missing)}")

def assert_schema_exists(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    if not check_schema_exists(spark, catalog, schema):
        raise ValueError(f"Missing schema: {catalog}.{schema}")

def assert_catalog_exists(
    spark: SparkSession,
    catalog: str,
) -> None:
    if not check_catalog_exists(spark, catalog):
        raise ValueError(f"Missing catalog: {catalog}")