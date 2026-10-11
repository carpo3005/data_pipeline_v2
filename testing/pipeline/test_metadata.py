from datetime import datetime, timezone

from pipeline.metadata import MetadataColumns, add_metadata, add_record_hash

SOURCE_COLUMNS = ["first_name", "last_name", "age"]
INGESTED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _hashes(df, column="_record_hash"):
    return {r.first_name: r[column] for r in df.collect()}


def test_add_record_hash_is_deterministic(sample_df):
    assert _hashes(add_record_hash(sample_df, SOURCE_COLUMNS)) == _hashes(add_record_hash(sample_df, SOURCE_COLUMNS))

def test_add_record_hash_differs_per_row(sample_df):
    hashes = _hashes(add_record_hash(sample_df, SOURCE_COLUMNS))
    assert len(set(hashes.values())) == sample_df.count()

def test_add_record_hash_ignores_columns_not_listed(sample_df):
    before = _hashes(add_record_hash(sample_df, SOURCE_COLUMNS))
    with_extra = add_record_hash(sample_df.withColumn("_run_id", sample_df.age), SOURCE_COLUMNS)
    assert _hashes(with_extra) == before

def test_add_metadata_adds_default_columns(sample_df):
    df = add_metadata(sample_df, run_id="run-1", ingested_at=INGESTED_AT, source_columns=SOURCE_COLUMNS)
    assert {"_record_hash", "_run_id", "_ingested_at"} <= set(df.columns)
    row = df.first()
    assert row._run_id == "run-1"
    assert row._ingested_at is not None

def test_add_metadata_hash_matches_hash_of_source_only(sample_df):
    df = add_metadata(sample_df, run_id="run-1", ingested_at=INGESTED_AT, source_columns=SOURCE_COLUMNS)
    assert _hashes(df) == _hashes(add_record_hash(sample_df, SOURCE_COLUMNS))

def test_add_metadata_uses_configured_names(sample_df):
    names = MetadataColumns(record_hash="h", run_id="r", ingested_at="i")
    df = add_metadata(sample_df, run_id="run-1", ingested_at=INGESTED_AT, source_columns=SOURCE_COLUMNS, columns=names)
    assert {"h", "r", "i"} <= set(df.columns)
    assert "_record_hash" not in df.columns
