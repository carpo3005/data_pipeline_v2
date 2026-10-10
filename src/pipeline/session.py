
import os

from pathlib import Path
from delta import configure_spark_with_delta_pip
from platformdirs import user_data_dir
from pyspark.sql import SparkSession

def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_spark_session(app_name: str = "data_pipeline", warehouse_path = None) -> SparkSession:
    """
    Create and return a SparkSession with the specified application name.

    Args:
        app_name (str): The name of the Spark application.
    """

    if is_databricks():
        return SparkSession.builder.getOrCreate()
    else:
        return _create_spark_local(app_name, warehouse_path)


def _create_spark_local(app_name: str = "data_pipeline", warehouse_path: Path = None) -> SparkSession:
    
    if warehouse_path is None:
        base_dir = Path(user_data_dir(appname="Project_Mercury"))
        warehouse_path = base_dir / "spark-warehouse"

    warehouse_path.mkdir(parents=True, exist_ok=True)
    metastore_path = warehouse_path.parent / "metastore_db"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_path.as_uri())
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_path};create=true",
        )
        .enableHiveSupport()
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark