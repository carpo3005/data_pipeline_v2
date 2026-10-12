-- ============================================================
-- DDL: fact_sales
-- Gold Layer — Sales Transaction Fact Table
-- ============================================================
-- Grain:            One row per order item
-- Source:           silver_order + silver_order_item
-- Dimension FKs:    customer_sk, billing_address_sk,
--                   shipping_address_sk, product_sk,
--                   order_date_sk
-- ============================================================

CREATE OR REPLACE TABLE forge.gold.fact_sales_test
(
    -- Surrogate key
    sales_sk BIGINT NOT NULL,

    -- Degenerate dimensions
    order_id STRING NOT NULL,
    order_item_id STRING NOT NULL,

    -- Foreign keys to dimensions
    customer_sk BIGINT NOT NULL,
    billing_address_sk BIGINT NOT NULL,
    shipping_address_sk BIGINT NOT NULL,
    product_sk BIGINT NOT NULL,
    order_date_sk BIGINT NOT NULL,

    -- Order context (from silver_order)
    channel STRING,
    order_status STRING,
    currency_code STRING,

    -- Measures (from silver_order_item)
    quantity INT,
    unit_price DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    tax_amount DECIMAL(18,2),
    line_total_amount DECIMAL(18,2),

    -- Audit / lineage
    _source_batch_id STRING NOT NULL,
    _pipeline_run_id STRING NOT NULL,
    _gold_loaded_timestamp TIMESTAMP NOT NULL,

    -- Constraints (informational — used by the optimiser)
    CONSTRAINT fact_sales_pk PRIMARY KEY (sales_sk),

    -- Foreign keys to dimension tables
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_sk) REFERENCES forge.gold.dim_customer_test(customer_sk),
    CONSTRAINT fk_fact_sales_billing_addr FOREIGN KEY (billing_address_sk) REFERENCES forge.gold.dim_address_test(address_sk),
    CONSTRAINT fk_fact_sales_shipping_addr FOREIGN KEY (shipping_address_sk) REFERENCES forge.gold.dim_address_test(address_sk),
    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_sk) REFERENCES forge.gold.dim_product_test(product_sk),
    CONSTRAINT fk_fact_sales_date FOREIGN KEY (order_date_sk) REFERENCES forge.gold.dim_date_test(date_sk)
)
USING DELTA;

ALTER TABLE forge.gold.fact_sales_test ADD CONSTRAINT fact_sales_bk UNIQUE (order_item_id);
ALTER TABLE forge.gold.fact_sales_test ADD CONSTRAINT chk_sales_quantity CHECK (quantity > 0);
ALTER TABLE forge.gold.fact_sales_test ADD CONSTRAINT chk_unit_price CHECK (unit_price >= 0);
ALTER TABLE forge.gold.fact_sales_test ADD CONSTRAINT chk_discount_amount CHECK (discount_amount >= 0);
ALTER TABLE forge.gold.fact_sales_test ADD CONSTRAINT chk_tax_amount CHECK (tax_amount >= 0);
ALTER TABLE forge.gold.fact_sales_test ADD CONSTRAINT chk_line_total CHECK (line_total_amount >= 0);
ALTER TABLE forge.gold.fact_sales_test ADD CONSTRAINT chk_order_status CHECK (order_status IN ('PENDING', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED'));
ALTER TABLE forge.gold.fact_sales_test ADD CONSTRAINT chk_currency_code CHECK (LENGTH(currency_code) = 3);
