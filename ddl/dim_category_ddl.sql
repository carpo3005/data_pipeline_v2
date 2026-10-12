-- ============================================================
-- DDL: dim_category
-- Gold Layer — SCD Type 1 Category Dimension
-- ============================================================
-- Grain:            One row per category (current state only)
-- Business Key:     category_id
-- Surrogate Key:    category_sk (deterministic xxhash64)
-- Change Detection: attribute_hash (SHA-256 of tracked columns)
-- SCD Strategy:     Type 1 — overwrites in place, no history
-- ============================================================

-- ============================================================
-- Step 1: Create the dim_category table
-- ============================================================
-- Defines the Gold-layer dimension with surrogate key, business
-- key, descriptive attributes, a change-detection hash, and
-- audit columns.  PK and UNIQUE constraints are informational
-- (not enforced) but used by the query optimiser.
-- ============================================================

CREATE OR REPLACE TABLE forge.gold.dim_category_test
(
    -- Surrogate key
    category_sk BIGINT NOT NULL,

    -- Business key
    category_id STRING NOT NULL,

    -- Category attributes
    category_name STRING NOT NULL,
    parent_category_id STRING,
    category_code STRING,
    is_active BOOLEAN,

    -- SCD Type 1 change-detection hash
    attribute_hash STRING NOT NULL,

    -- Audit metadata
    _source_batch_id STRING NOT NULL,
    _pipeline_run_id STRING NOT NULL,
    _gold_loaded_timestamp TIMESTAMP NOT NULL,

    -- Constraints (informational — used by the optimiser)
    CONSTRAINT dim_category_pk PRIMARY KEY (category_sk)
)
USING DELTA;


-- ============================================================
-- Step 2: Seed the unknown / default member row
-- ============================================================
-- Inserts a single "unknown" row (category_sk = -1) used by
-- fact tables when a category lookup fails.  The MERGE ensures
-- idempotency — the row is only inserted if it does not already
-- exist.
-- ============================================================

MERGE INTO forge.gold.dim_category_test AS tgt_tbl
USING
(
    SELECT
        CAST(-1 AS BIGINT)  AS category_sk,
        '-1'                AS category_id,
        'UNKNOWN'           AS category_name,
        NULL                AS parent_category_id,
        'UNKNOWN'           AS category_code,
        FALSE               AS is_active,
        'SYSTEM'            AS attribute_hash,
        'SYSTEM'            AS _batch_id,
        current_timestamp() AS _gold_loaded_timestamp
) AS src_tbl
ON tgt_tbl.category_sk = src_tbl.category_sk
WHEN NOT MATCHED
THEN INSERT *;

ALTER TABLE forge.gold.dim_category_test ADD CONSTRAINT dim_category_bk UNIQUE (category_id);