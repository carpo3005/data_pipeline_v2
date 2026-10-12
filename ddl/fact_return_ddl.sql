-- ============================================================
-- DDL: fact_return
-- Gold Layer — Return Transaction Fact Table
-- ============================================================
-- Grain:            One row per return item
-- Source:           silver_return + silver_return_item
-- Dimension FKs:    customer_sk, product_sk, return_date_sk
-- ============================================================

CREATE OR REPLACE TABLE forge.gold.fact_return_test
(
    -- Surrogate key
    return_sk BIGINT NOT NULL,

    -- Degenerate dimensions
    return_id STRING NOT NULL,
    return_item_id STRING NOT NULL,
    order_item_id STRING NOT NULL,

    -- Foreign keys to dimensions
    customer_sk BIGINT NOT NULL,
    product_sk BIGINT NOT NULL,
    return_date_sk BIGINT NOT NULL,

    -- Return attributes
    return_reason STRING,
    return_status STRING,

    -- Measures
    quantity INT,
    refund_amount DECIMAL(18,2),

    -- Audit / lineage
    _source_batch_id STRING NOT NULL,
    _pipeline_run_id STRING NOT NULL,
    _gold_loaded_timestamp TIMESTAMP NOT NULL,

    -- Constraints (informational — used by the optimiser)
    CONSTRAINT fact_return_pk PRIMARY KEY (return_sk),

    -- Foreign keys to dimension tables
    CONSTRAINT fk_fact_return_customer FOREIGN KEY (customer_sk) REFERENCES forge.gold.dim_customer_test(customer_sk),
    CONSTRAINT fk_fact_return_product FOREIGN KEY (product_sk) REFERENCES forge.gold.dim_product_test(product_sk),
    CONSTRAINT fk_fact_return_date FOREIGN KEY (return_date_sk) REFERENCES forge.gold.dim_date_test(date_sk)
)
USING DELTA;

ALTER TABLE forge.gold.fact_return_test ADD CONSTRAINT fact_return_bk UNIQUE (return_item_id);
ALTER TABLE forge.gold.fact_return_test ADD CONSTRAINT chk_return_quantity CHECK (quantity > 0);
ALTER TABLE forge.gold.fact_return_test ADD CONSTRAINT chk_refund_amount CHECK (refund_amount >= 0);
ALTER TABLE forge.gold.fact_return_test ADD CONSTRAINT chk_return_status CHECK (return_status IN ('PENDING', 'APPROVED', 'REJECTED', 'COMPLETED'));
