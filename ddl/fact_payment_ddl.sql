-- ============================================================
-- DDL: fact_payment
-- Gold Layer — Payment Transaction Fact Table
-- ============================================================
-- Grain:            One row per payment
-- Source:           silver_payment
-- Dimension FKs:    customer_sk, payment_date_sk
-- ============================================================

CREATE OR REPLACE TABLE forge.gold.fact_payment_test
(
    -- Surrogate key
    payment_sk BIGINT NOT NULL,

    -- Degenerate dimensions
    payment_id STRING NOT NULL,
    order_id STRING NOT NULL,

    -- Foreign keys to dimensions
    customer_sk BIGINT NOT NULL,
    payment_date_sk BIGINT NOT NULL,

    -- Payment attributes
    payment_method STRING,
    payment_status STRING,
    currency_code STRING,
    transaction_reference STRING,

    -- Measures
    payment_amount DECIMAL(18,2),

    -- Audit / lineage
    _source_batch_id STRING NOT NULL,
    _pipeline_run_id STRING NOT NULL,
    _gold_loaded_timestamp TIMESTAMP NOT NULL,

    -- Constraints (informational — used by the optimiser)
    CONSTRAINT fact_payment_pk PRIMARY KEY (payment_sk),

    -- Foreign keys to dimension tables
    CONSTRAINT fk_fact_payment_customer FOREIGN KEY (customer_sk) REFERENCES forge.gold.dim_customer_test(customer_sk),
    CONSTRAINT fk_fact_payment_date FOREIGN KEY (payment_date_sk) REFERENCES forge.gold.dim_date_test(date_sk)
)
USING DELTA;

ALTER TABLE forge.gold.fact_payment_test ADD CONSTRAINT fact_payment_bk UNIQUE (payment_id);
ALTER TABLE forge.gold.fact_payment_test ADD CONSTRAINT chk_payment_amount CHECK (payment_amount >= 0);
ALTER TABLE forge.gold.fact_payment_test ADD CONSTRAINT chk_payment_status CHECK (payment_status IN ('PENDING', 'COMPLETED', 'FAILED', 'REFUNDED'));
ALTER TABLE forge.gold.fact_payment_test ADD CONSTRAINT chk_currency_code CHECK (LENGTH(currency_code) = 3);
