# DQ rules: return_item

Source: `silver_stage.return_item` → `silver.return_item` / `quarantine.return_item`. Prefix `RIT`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| RIT-001 | H | completeness | not_null | return_item_id | `return_item_id` is present |  |
| RIT-002 | H | completeness | not_null | return_id | `return_id` is present |  |
| RIT-003 | H | completeness | not_null | order_item_id | `order_item_id` is present |  |
| RIT-004 | H | completeness | not_null | quantity | `quantity` is present |  |
| RIT-005 | H | validity | in_range | quantity | quantity is at least 1 | minimum 1 |
| RIT-006 | H | completeness | not_null | refund_amount | `refund_amount` is present |  |
| RIT-007 | H | validity | in_range | refund_amount | refund is not negative | minimum 0 |
| RIT-008 | S | validity | in_set | return_reason | reason is known | DEFECTIVE, UNWANTED, WRONG_ITEM, SIZE_FIT, LATE_DELIVERY, DAMAGED |
| RIT-009 | S | completeness | not_null | product_id | `product_id` is present |  |
| RIT-010 | S | referential | fk_exists | return_id | return exists | Phase 4 |
| RIT-011 | S | referential | fk_exists | order_item_id | order item exists | Phase 4 |
| RIT-012 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
