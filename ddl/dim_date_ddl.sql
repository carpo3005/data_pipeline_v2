-- ============================================================
-- DDL: dim_date
-- Gold Layer — SCD Type 0 Date Dimension
-- ============================================================
-- Grain:            One row per calendar date
-- Business Key:     date_key (YYYYMMDD integer)
-- Surrogate Key:    date_sk (deterministic xxhash64)
-- SCD Strategy:     Type 0 — immutable, loaded once
-- ============================================================

CREATE OR REPLACE TABLE forge.gold.dim_date_test
(
    -- Surrogate key
    date_sk BIGINT NOT NULL,

    -- Business key (YYYYMMDD integer format)
    date_key INT NOT NULL,

    -- Calendar attributes
    full_date DATE,
    year INT,
    quarter INT,
    quarter_name STRING,
    month INT,
    month_name STRING,
    month_short_name STRING,
    week_of_year INT,
    iso_day_of_week INT,
    day_of_month INT,
    day_of_year INT,
    day_name STRING,
    year_month STRING,
    year_quarter STRING,
    calendar_month_start_date DATE,
    calendar_quarter_start_date DATE,
    calendar_year_start_date DATE,
    is_weekend BOOLEAN,
    is_month_start BOOLEAN,
    is_month_end BOOLEAN,
    is_quarter_start BOOLEAN,
    is_quarter_end BOOLEAN,
    is_year_start BOOLEAN,
    is_year_end BOOLEAN,
    _batch_id STRING NOT NULL,
    _gold_loaded_timestamp TIMESTAMP NOT NULL,

    -- Constraints (informational — used by the optimiser)
    CONSTRAINT dim_date_pk PRIMARY KEY (date_sk)
)
USING DELTA;

INSERT INTO forge.gold.dim_date_test
SELECT
    xxhash64(date_key) AS date_sk,
    date_key,
    full_date,
    year,
    quarter,
    quarter_name,
    month,
    month_name,
    month_short_name,
    week_of_year,
    iso_day_of_week,
    day_of_month,
    day_of_year,
    day_name,
    year_month,
    year_quarter,
    calendar_month_start_date,
    calendar_quarter_start_date,
    calendar_year_start_date,
    is_weekend,
    is_month_start,
    is_month_end,
    is_quarter_start,
    is_quarter_end,
    is_year_start,
    is_year_end,
    'SYSTEM' AS _batch_id,
    current_timestamp() AS _gold_loaded_timestamp
FROM (
    SELECT

        CAST(
            YEAR(full_date) * 10000
            + MONTH(full_date) * 100
            + DAY(full_date) AS INT
        ) AS date_key,

        full_date,
        YEAR(full_date) AS year,
        QUARTER(full_date) AS quarter,
        CONCAT('Q', QUARTER(full_date)) AS quarter_name,
        MONTH(full_date) AS month,
        DATE_FORMAT(full_date, 'MMMM') AS month_name,
        DATE_FORMAT(full_date, 'MMM') AS month_short_name,
        WEEKOFYEAR(full_date) AS week_of_year,
        WEEKDAY(full_date) + 1  AS iso_day_of_week,
        DAY(full_date) AS day_of_month,
        DAYOFYEAR(full_date) AS day_of_year,
        DATE_FORMAT(full_date, 'EEEE') AS day_name,
        DATE_FORMAT(full_date, 'yyyy-MM') AS year_month,
        CONCAT(YEAR(full_date), '-Q', QUARTER(full_date)) AS year_quarter,
        TRUNC(full_date, 'MM') AS calendar_month_start_date,
        CAST(DATE_TRUNC('QUARTER', full_date) AS DATE) AS calendar_quarter_start_date,
        TRUNC(full_date, 'YEAR') AS calendar_year_start_date,
        WEEKDAY(full_date) >= 5 AS is_weekend,
        DAY(full_date) = 1 AS is_month_start,
        full_date = LAST_DAY(full_date) AS is_month_end,
        MONTH(full_date) IN (1, 4, 7, 10) AND DAY(full_date) = 1 AS is_quarter_start,
        MONTH(full_date) IN (3, 6, 9, 12) AND full_date = LAST_DAY(full_date) AS is_quarter_end,
        MONTH(full_date) = 1  AND DAY(full_date) = 1 AS is_year_start,
        MONTH(full_date) = 12 AND DAY(full_date) = 31 AS is_year_end

    FROM (
        SELECT EXPLODE(SEQUENCE(DATE'2024-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS full_date
    ) date_spine
) date_attrs;


MERGE INTO forge.gold.dim_date_test AS date_tgt
USING
(
    SELECT
        CAST(-1 AS BIGINT)      AS date_sk,
        CAST(-1 AS INT)         AS date_key,
        CAST(NULL AS DATE)      AS full_date,
        CAST(NULL AS INT)       AS year,
        CAST(NULL AS INT)       AS quarter,
        'UNKNOWN'               AS quarter_name,
        CAST(NULL AS INT)       AS month,
        'UNKNOWN'               AS month_name,
        'UNK'                   AS month_short_name,
        CAST(NULL AS INT)       AS week_of_year,
        CAST(NULL AS INT)       AS iso_day_of_week,
        CAST(NULL AS INT)       AS day_of_month,
        CAST(NULL AS INT)       AS day_of_year,
        'UNKNOWN'               AS day_name,
        NULL                    AS year_month,
        NULL                    AS year_quarter,
        CAST(NULL AS DATE)      AS calendar_month_start_date,
        CAST(NULL AS DATE)      AS calendar_quarter_start_date,
        CAST(NULL AS DATE)      AS calendar_year_start_date,
        FALSE                   AS is_weekend,
        FALSE                   AS is_month_start,
        FALSE                   AS is_month_end,
        FALSE                   AS is_quarter_start,
        FALSE                   AS is_quarter_end,
        FALSE                   AS is_year_start,
        FALSE                   AS is_year_end,
        'SYSTEM'                AS _batch_id,
        current_timestamp()     AS _gold_loaded_timestamp
) AS date_src
ON date_tgt.date_sk = date_src.date_sk
WHEN NOT MATCHED
THEN INSERT *;

ALTER TABLE forge.gold.dim_date_test ADD CONSTRAINT dim_date_bk UNIQUE (date_key);
