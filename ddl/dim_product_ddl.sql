-- ============================================================
-- DDL: dim_product
-- Gold Layer — SCD Type 2 Product Dimension
-- ============================================================
-- Grain:            One row per product version
-- Business Key:     product_id
-- Surrogate Key:    product_sk (deterministic xxhash64)
-- Change Detection: attribute_hash (SHA-256 of tracked columns)
-- ============================================================

CREATE OR REPLACE TABLE forge.gold.dim_product_test
(
    -- Surrogate key
    -- Deterministic hash of (product_id, effective_start_date)
    product_sk BIGINT NOT NULL,

    -- Business key
    product_id STRING NOT NULL,

    -- Product attributes
    sku STRING,
    product_name STRING,
    brand STRING,
    unit_price DECIMAL(18,4),
    currency_code STRING,
    is_active BOOLEAN,

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

    -- constraints
    CONSTRAINT pk_product_sk PRIMARY KEY (product_sk)
    
)
USING DELTA

 CLUSTER BY (
        product_id,
        effective_start_date,
        effective_end_date
);

MERGE INTO
        forge.gold.dim_product_test 
    AS tgt_tbl
    USING
    (
        SELECT
            CAST(-1 AS BIGINT) AS product_sk,
            '-1' AS product_id,
            '-1' AS sku,
            'UNKNOWN' AS product_name,
            'UNKNOWN' AS brand,
            CAST(0 AS DECIMAL(18,4)) AS unit_price,
            'UNKNOWN' AS currency_code,
            FALSE AS is_active,
            'SYSTEM' AS attribute_hash,
            TIMESTAMP '1900-01-01 00:00:00' AS effective_start_date,
            TIMESTAMP '9999-12-31 00:00:00' AS effective_end_date,
            TRUE AS is_current,
            0 AS row_version,
            'SYSTEM' AS _batch_id,
            current_timestamp() AS _gold_loaded_timestamp
    ) AS src_tbl

    ON tgt_tbl.product_sk = src_tbl.product_sk
    WHEN NOT MATCHED
    THEN INSERT *;

ALTER TABLE forge.gold.dim_product_test ADD CONSTRAINT ck_dim_product_effective_range CHECK (effective_start_date < effective_end_date);
ALTER TABLE forge.gold.dim_product_test ADD CONSTRAINT ck_dim_product_price CHECK (unit_price >= 0);
ALTER TABLE forge.gold.dim_product_test ADD CONSTRAINT ck_dim_product_row_version CHECK (row_version >= 0)
