from silver.curation import add_lineage_columns, deduplicate, source_columns


def test_source_columns_excludes_metadata(spark):
    df = spark.createDataFrame([("1", "x", "a/b.csv")], "customer_id string, name string, _source_file string")
    assert source_columns(df) == ["customer_id", "name"]


def test_lineage_columns_are_added(spark):
    df = spark.createDataFrame(
        [("1", "x", "data/B001/customer.csv", "run-1")],
        "customer_id string, name string, _source_file string, _run_id string",
    )
    row = add_lineage_columns(df, source_system="CRM", pipeline_run_id="silver-1").collect()[0]
    assert row["_source_file_name"] == "customer.csv"
    assert row["_source_file_path"] == "data/B001/customer.csv"
    assert row["_source_batch_id"] == "run-1"
    assert row["_source_system"] == "CRM"
    assert row["_pipeline_run_id"] == "silver-1"
    assert row["_raw_record_payload"] == '{"customer_id":"1","name":"x"}'


def test_deduplicate_removes_null_keys_hash_duplicates_and_superseded(spark):
    df = spark.createDataFrame(
        [
            ("1", "old", "h1", "2024-01-01 00:00:00", None),
            ("1", "new", "h2", "2024-06-01 00:00:00", None),
            ("1", "new", "h2", "2024-06-01 00:00:00", None),   # exact duplicate of the row above
            (None, "orphan", "h3", "2024-01-01 00:00:00", None),  # null business key
            ("2", "only", "h4", "2024-01-01 00:00:00", None),
        ],
        "customer_id string, name string, _record_hash string, updated_at string, _ingested_at string",
    ).selectExpr("customer_id", "name", "_record_hash", "cast(updated_at as timestamp) updated_at", "_ingested_at")

    latest, counts = deduplicate(df, ["customer_id"])
    rows = sorted((r.customer_id, r.name) for r in latest.collect())
    assert rows == [("1", "new"), ("2", "only")]
    assert counts == {
        "null_business_key_removed_count": 1,
        "duplicate_hash_removed_count": 1,
        "superseded_removed_count": 1,
    }
