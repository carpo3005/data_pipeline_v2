-- ============================================================
-- DDL: dim_address
-- Gold Layer — SCD Type 2 Address Dimension
-- ============================================================
-- Grain:           One row per address version
-- Business Key:    address_id
-- Surrogate Key:   address_sk (deterministic xxhash64)
-- Change Detection: attribute_hash (SHA-256 of tracked columns)
-- ============================================================

CREATE OR REPLACE TABLE forge.gold.dim_address_test
(
    -- Surrogate key
    -- Deterministic hash of (address_id, effective_start_date)
    address_sk BIGINT NOT NULL,

    -- Business key
    address_id STRING NOT NULL,

    -- Foreign key to dim_customer
    customer_id STRING NOT NULL,

    -- Address attributes
    address_type STRING,
    address_line_1 STRING,
    address_line_2 STRING,
    city STRING,
    state_region STRING,
    country_subdivision_code STRING,
    postal_code STRING,
    country_code STRING,
    is_primary BOOLEAN,

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
    CONSTRAINT pk_dim_address PRIMARY KEY (address_sk)

)
USING DELTA

CLUSTER BY (
    address_id,
    effective_start_date,
    effective_end_date
);

MERGE INTO 
        forge.gold.dim_address_test
    AS tgt_tbl
    USING
    (
        SELECT
            CAST(-1 AS BIGINT) AS address_sk,
            '-1' AS address_id,
            '-1' AS customer_id,
            'UNKNOWN' AS address_type,
            'UNKNOWN' AS address_line_1,
            NULL AS address_line_2,
            'UNKNOWN' AS city,
            NULL AS state_region,
            NULL AS country_subdivision_code,
            NULL AS postal_code,
            NULL AS country_code,
            FALSE AS is_primary,
            'SYSTEM' AS attribute_hash,
            TIMESTAMP '1900-01-01 00:00:00' AS effective_start_date,
            TIMESTAMP '9999-12-31 00:00:00' AS effective_end_date,
            TRUE AS is_current,
            0 AS row_version,
            'SYSTEM' AS _batch_id,
            current_timestamp() AS _gold_loaded_timestamp
    ) AS src_tbl

    ON tgt_tbl.address_sk = src_tbl.address_sk
    WHEN NOT MATCHED
    THEN INSERT *;

ALTER TABLE forge.gold.dim_address_test ADD CONSTRAINT ck_dim_address_row_version CHECK (row_version >= 0);
ALTER TABLE forge.gold.dim_address_test ADD CONSTRAINT ck_dim_address_effective_range CHECK (effective_end_date >= effective_start_date)