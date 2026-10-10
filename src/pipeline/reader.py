from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read_table(
        spark: SparkSession,
        table_fqn: str,
) -> DataFrame:
    return spark.read.table(table_fqn)

def read_source_csv(
    spark: SparkSession,
    schema: StructType,
    path: str,
) -> DataFrame:
    """Read one source CSV with the ingestion CSV options."""
    
    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .option("delimiter", ",")
        .option("multiline", "true")
        .option("mode", "permissive")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(path)
    )
    return df

