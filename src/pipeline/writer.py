from pyspark.sql import DataFrame

def overwrite_table(df: DataFrame, table_fqn: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_fqn)
    )

def append_table(df: DataFrame, table_fqn: str) -> None:
    df.write.format("delta").mode("append").saveAsTable(table_fqn)

def create_delta_table(
    df: DataFrame,
    table_fqn: str,
) -> None:
    df.write.format("delta").mode("error").saveAsTable(table_fqn)