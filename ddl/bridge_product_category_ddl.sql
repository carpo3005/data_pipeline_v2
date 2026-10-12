-- ============================================================
-- DDL: bridge_product_category
-- Gold Layer — Product-Category Many-to-Many Bridge
-- ============================================================
-- Grain:            One row per product–category assignment
-- Business Key:     product_category_id
-- Surrogate Key:    bridge_sk (deterministic xxhash64)
-- Source:           silver_product_category
-- ============================================================

CREATE OR REPLACE TABLE forge.gold.bridge_product_category_test
(
    -- Surrogate key
    bridge_sk BIGINT NOT NULL,

    -- Business key
    product_category_id STRING NOT NULL,

    -- Foreign keys to dimensions
    product_sk BIGINT NOT NULL,
    category_sk BIGINT NOT NULL,

    -- Bridge attributes
    is_primary BOOLEAN,
    active_from DATE,
    active_to DATE,

    -- Audit / lineage
    _source_batch_id STRING NOT NULL,
    _pipeline_run_id STRING NOT NULL,
    _gold_loaded_timestamp TIMESTAMP NOT NULL,

    -- Constraints
    CONSTRAINT bridge_product_category_pk PRIMARY KEY (bridge_sk),
    CONSTRAINT fk_bridge_product FOREIGN KEY (product_sk) REFERENCES forge.gold.dim_product_test(product_sk),
    CONSTRAINT fk_bridge_category FOREIGN KEY (category_sk) REFERENCES forge.gold.dim_category_test(category_sk)
)
USING DELTA
CLUSTER BY (product_sk, category_sk);

ALTER TABLE forge.gold.bridge_product_category_test ADD CONSTRAINT bridge_product_category_bk UNIQUE (product_category_id);
