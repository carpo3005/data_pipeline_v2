# DQ rules: product

Source: `silver_stage.product` → `silver.product` / `quarantine.product`. Prefix `PRD`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| PRD-001 | H | completeness | not_null | product_id | `product_id` is present |  |
| PRD-002 | H | completeness | not_null | sku | `sku` is present |  |
| PRD-003 | H | completeness | not_null | unit_price | `unit_price` is present |  |
| PRD-004 | H | validity | in_range | unit_price | unit price is not negative | minimum 0 |
| PRD-005 | S | completeness | not_null | product_name | `product_name` is present |  |
| PRD-006 | S | validity | in_set | currency_code | currency is known | GBP |
| PRD-007 | S | validity | is_null | is_active_raw | is_active is a recognised boolean | standardisation could not parse the value |
| PRD-008 | S | completeness | not_null | brand | `brand` is present |  |
| PRD-009 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
