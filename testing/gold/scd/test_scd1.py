from gold.scd.scd1 import merge_scd1


def test_scd1_overwrites_only_when_the_attribute_hash_changes(isolated_spark):
    name = "scd1_test_category"
    isolated_spark.sql(f"DROP TABLE IF EXISTS {name}")
    isolated_spark.sql(f"""
        CREATE TABLE {name} (category_sk BIGINT, category_id STRING, category_name STRING,
            attribute_hash STRING, _source_batch_id STRING, _pipeline_run_id STRING,
            _gold_loaded_timestamp TIMESTAMP) USING DELTA
    """)
    isolated_spark.sql(f"""INSERT INTO {name} VALUES
        (1, 'c1', 'Old', 'h1', 'b', 'r1', NULL), (2, 'c2', 'Same', 'h2', 'b', 'r1', NULL)""")
    incoming = isolated_spark.createDataFrame(
        [(1, "c1", "New", "h1x"), (2, "c2", "Same", "h2"), (3, "c3", "Fresh", "h3")],
        "category_sk bigint, category_id string, category_name string, attribute_hash string",
    ).selectExpr("*", "'b' AS _source_batch_id", "'r2' AS _pipeline_run_id", "CAST(NULL AS TIMESTAMP) AS _gold_loaded_timestamp")

    result = merge_scd1(isolated_spark, name, incoming, business_key="category_id")
    assert (result.inserted, result.updated, result.unchanged) == (1, 1, 1)
    names = {r["category_id"]: r["category_name"] for r in isolated_spark.table(name).collect()}
    assert names == {"c1": "New", "c2": "Same", "c3": "Fresh"}
