# DQ rules: order_item

Source: `silver_stage.order_item` → `silver.order_item` / `quarantine.order_item`. Prefix `OIT`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| OIT-001 | H | completeness | not_null | order_item_id | `order_item_id` is present |  |
| OIT-002 | H | completeness | not_null | quantity | quantity is present (`TWO` fails to parse and lands here) |  |
| OIT-003 | H | validity | in_range | quantity | quantity is at least 1 | minimum 1 |
| OIT-004 | H | completeness | not_null | order_id | `order_id` is present |  |
| OIT-005 | H | completeness | not_null | product_id | `product_id` is present |  |
| OIT-006 | H | validity | in_range | unit_price | unit price is not negative | minimum 0 |
| OIT-007 | S | consistency | sql_expression | quantity, unit_price, discount_amount, tax_amount, line_total_amount | line total = quantity x unit price - discount + tax | `abs(quantity * unit_price - discount_amount + tax_amount - line_total_amount) <= 0.01` |
| OIT-008 | S | referential | fk_exists | order_id | order exists | Phase 4 |
| OIT-009 | S | referential | fk_exists | product_id | product exists | Phase 4 |
| OIT-010 | S | validity | in_range | discount_amount | discount is not negative | minimum 0 |
| OIT-011 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
