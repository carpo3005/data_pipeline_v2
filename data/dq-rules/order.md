# DQ rules: order

Source: `silver_stage.order` → `silver.order` / `quarantine.order`. Prefix `ORD`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| ORD-001 | H | completeness | not_null | order_id | `order_id` is present |  |
| ORD-002 | H | completeness | not_null | customer_id | `customer_id` is present |  |
| ORD-003 | H | completeness | not_null | order_timestamp | `order_timestamp` is present |  |
| ORD-004 | H | validity | in_range | order_total_amount | order total is not negative | minimum 0 |
| ORD-005 | S | validity | in_set | order_status | status is known | COMPLETED, CANCELLED, PENDING |
| ORD-006 | S | validity | in_set | channel | channel is known | STORE, ECOMMERCE, MOBILE |
| ORD-007 | S | validity | in_set | currency_code | currency is known | GBP |
| ORD-008 | S | consistency | sql_expression | subtotal_amount, discount_amount, tax_amount, shipping_amount, order_total_amount | total = subtotal - discount + tax + shipping | `abs(subtotal_amount - discount_amount + tax_amount + shipping_amount - order_total_amount) <= 0.01` |
| ORD-009 | H | plausibility | not_future | order_timestamp | order is not in the future |  |
| ORD-010 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |
| ORD-011 | S | referential | fk_exists | customer_id | customer exists | Phase 4 |
| ORD-012 | H | completeness | not_null | order_total_amount | `order_total_amount` is present |  |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
