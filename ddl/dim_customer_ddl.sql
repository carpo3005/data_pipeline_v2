-- ============================================================
-- DDL: dim_customer
-- Gold Layer — SCD Type 2 Customer Dimension
-- ============================================================
-- Grain:            One row per customer version
-- Business Key:     customer_id
-- Surrogate Key:    customer_sk (deterministic xxhash64)
-- Change Detection: attribute_hash (SHA-256 of tracked columns)
-- ============================================================

CREATE OR REPLACE TABLE forge.gold.dim_customer_test
(
    -- Surrogate key
    -- Deterministic hash of (customer_id, effective_start_date)
    customer_sk BIGINT NOT NULL,

    -- Business key
    customer_id STRING NOT NULL,

    -- Customer attributes
    first_name STRING,
    last_name STRING,
    email_hash STRING,  -- salted SHA-256 of the normalised email; the raw address is never stored in Gold
    email_domain STRING,
    phone STRING,
    date_of_birth DATE,
    loyalty_status STRING,
    marketing_opt_in BOOLEAN,

    -- SCD Type 2 change detection
    attribute_hash STRING NOT NULL,

    -- SCD Type 2 versioning metadata
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL,
    row_version INT NOT NULL,

    -- Audit / lineage
    _source_batch_id STRING NOT NULL,
    _pipeline_run_id STRING NOT NULL,
    _gold_loaded_timestamp TIMESTAMP NOT NULL,

    -- Constraints
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
)
USING DELTA
CLUSTER BY (
        customer_id,
        effective_start_date
    );

MERGE INTO
        forge.gold.dim_customer_test
        AS tgt_tbl
    USING
    (
        SELECT
            CAST(-1 AS BIGINT) AS customer_sk,
            '-1' AS customer_id,
            'UNKNOWN' AS first_name,
            'UNKNOWN' AS last_name,
            NULL AS email_hash,
            NULL AS email_domain,
            NULL AS phone,
            NULL AS date_of_birth,
            'UNKNOWN' AS loyalty_status,
            FALSE AS marketing_opt_in,
            'SYSTEM' AS attribute_hash,
            TIMESTAMP '1900-01-01 00:00:00' AS effective_start_date,
            TIMESTAMP '9999-12-31 00:00:00' AS effective_end_date,
            TRUE AS is_current,
            0 AS row_version,
            'SYSTEM' AS _source_batch_id,
            'SYSTEM' AS _pipeline_run_id,
            current_timestamp() AS _gold_loaded_timestamp
    ) AS src_tbl

    ON tgt_tbl.customer_sk = src_tbl.customer_sk
    WHEN NOT MATCHED
    THEN INSERT *;

ALTER TABLE forge.gold.dim_customer_test ADD CONSTRAINT ck_dim_customer_effective_dates CHECK (effective_start_date <= effective_end_date);
ALTER TABLE forge.gold.dim_customer_test ADD CONSTRAINT ck_dim_customer_row_version CHECK (row_version >= 0)

