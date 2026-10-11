from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType

DEFAULT_CSV_OPTIONS = {
    "header": "true",
    "delimiter": ",",
    "multiline": "true",
    "mode": "permissive",
    "columnNameOfCorruptRecord": "_corrupt_record",
}


def read_table(
        spark: SparkSession,
        table_fqn: str,
) -> DataFrame:
    return spark.read.table(table_fqn)

def read_source_csv(
    spark: SparkSession,
    schema: StructType,
    path: str,
    options: dict[str, str] | None = None,
    file_path_column: str | None = None,
) -> DataFrame:
    """Read source CSV files. `options` override DEFAULT_CSV_OPTIONS.

    If file_path_column is given, the path of each row's source file is added under that name.
    """
    df = (
        spark.read
        .schema(schema)
        .options(**{**DEFAULT_CSV_OPTIONS, **(options or {})})
        .csv(path)
    )
    if file_path_column:
        df = df.select("*", F.col("_metadata.file_path").alias(file_path_column))
    return df
