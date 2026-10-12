import hashlib
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import pytest

import config
from gold.ddl import run_ddl
from gold.dimensions import CUSTOMER, shape_customer
from gold.scd.scd2 import merge_scd2
from pipeline.catalog import ensure_schema_exists, get_table_fqn

SALT = "test-salt"
DDL_PATH = Path(__file__).resolve().parents[2] / "ddl" / "dim_customer_ddl.sql"
CUSTOMER_COLUMNS = (
    "customer_id string, first_name string, last_name string, email string, phone string, "
    "date_of_birth date, loyalty_status string, marketing_opt_in boolean, _source_batch_id string, "
    "_change_ts timestamp, _first_change_ts timestamp"
)
T1 = datetime(2024, 1, 1)
T3 = datetime(2024, 9, 1)


def _customer(customer_id, email, change_ts=T1):
    return (customer_id, "Ann", "Lee", email, "0123", date(1990, 1, 2), "GOLD", True, "B001", change_ts, T1)


def _customers(spark, rows):
    return spark.createDataFrame(rows, CUSTOMER_COLUMNS)


def test_email_hash_is_salted_sha256_of_normalised_email(spark):
    out = shape_customer(_customers(spark, [_customer("C1", "Ann.Lee@Example.COM ")]), SALT).collect()[0]
    expected = hashlib.sha256((SALT + "ann.lee@example.com").encode()).hexdigest()
    assert out["email_hash"] == expected
    assert out["email_domain"] == "example.com"


def test_raw_email_is_not_in_any_gold_column(spark):
    out = shape_customer(_customers(spark, [_customer("C1", "ann.lee@example.com")]), SALT)
    assert "email" not in out.columns
    assert "email_hash" in out.columns
    row = out.first()
    assert not any("ann.lee" in str(value) for value in row)


def test_same_address_with_different_case_or_spaces_gives_same_hash(spark):
    out = shape_customer(
        _customers(spark, [_customer("C1", "ann.lee@example.com"), _customer("C2", "  ANN.LEE@example.com ")]),
        SALT,
    )
    hashes = {row["customer_id"]: row["email_hash"] for row in out.collect()}
    assert hashes["C1"] == hashes["C2"]


def test_blank_or_null_email_gives_null_hash_and_domain(spark):
    out = shape_customer(
        _customers(spark, [_customer("C1", None), _customer("C2", "   ")]),
        SALT,
    )
    for row in out.collect():
        assert row["email_hash"] is None
        assert row["email_domain"] is None


def test_shape_customer_rejects_empty_salt(spark):
    with pytest.raises(ValueError, match="email_salt"):
        shape_customer(_customers(spark, [_customer("C1", "ann.lee@example.com")]), "")


def test_get_email_mask_salt_reads_environment(monkeypatch):
    monkeypatch.setenv(config.EMAIL_MASK_SALT_VAR, "from-env")
    assert config.get_email_mask_salt() == "from-env"


def test_get_email_mask_salt_fails_closed_when_unset(monkeypatch):
    monkeypatch.delenv(config.EMAIL_MASK_SALT_VAR, raising=False)
    with pytest.raises(RuntimeError, match=config.EMAIL_MASK_SALT_VAR):
        config.get_email_mask_salt()


def test_masked_customers_merge_into_scd2_dimension_without_raw_email(spark):
    schema = "gold_test_email_mask"
    table_fqn = get_table_fqn("forge", schema, f"dim_customer_{uuid4().hex[:8]}")
    ensure_schema_exists(spark, "forge", schema)
    ddl = DDL_PATH.read_text().replace("forge.gold.dim_customer_test", table_fqn)
    try:
        run_ddl(spark, ddl, gold=schema, databricks=False)

        first = merge_scd2(
            spark, table_fqn, CUSTOMER,
            shape_customer(_customers(spark, [_customer("C1", "ann.lee@example.com")]), SALT),
            run_id="test-run-1",
        )
        assert first.new_keys == 1

        table = spark.table(table_fqn)
        assert "email" not in table.columns
        assert "email_hash" in table.columns
        assert not any("@" in str(value) for row in table.collect() for value in row)

        # Same address, different formatting, same timestamp: nothing changes, no new version
        again = merge_scd2(
            spark, table_fqn, CUSTOMER,
            shape_customer(_customers(spark, [_customer("C1", " ANN.LEE@example.com", T1)]), SALT),
            run_id="test-run-2",
        )
        assert again.unchanged_rows == 1
        assert again.changed_keys == 0

        # A genuinely new address opens a new version and closes the old one
        changed = merge_scd2(
            spark, table_fqn, CUSTOMER,
            shape_customer(_customers(spark, [_customer("C1", "ann.new@example.com", T3)]), SALT),
            run_id="test-run-3",
        )
        assert changed.changed_keys == 1
        versions = spark.table(table_fqn).where("customer_id = 'C1'")
        assert versions.count() == 2
        assert versions.where("is_current").count() == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
