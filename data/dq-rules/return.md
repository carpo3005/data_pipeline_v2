# DQ rules: return

Source: `silver_stage.return` → `silver.return` / `quarantine.return`. Prefix `RET`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| RET-001 | H | completeness | not_null | return_id | `return_id` is present |  |
| RET-002 | H | completeness | not_null | order_id | `order_id` is present |  |
| RET-003 | H | completeness | not_null | customer_id | `customer_id` is present |  |
| RET-004 | H | completeness | not_null | return_timestamp | `return_timestamp` is present |  |
| RET-005 | H | completeness | not_null | total_refund_amount | `total_refund_amount` is present |  |
| RET-006 | H | validity | in_range | total_refund_amount | refund is not negative | minimum 0 |
| RET-007 | S | validity | in_set | return_status | status is known | COMPLETED, PENDING, REJECTED |
| RET-008 | S | validity | in_set | return_reason | reason is known | DEFECTIVE, UNWANTED, WRONG_ITEM, SIZE_FIT, LATE_DELIVERY, DAMAGED |
| RET-009 | H | plausibility | not_future | return_timestamp | return is not in the future |  |
| RET-010 | S | referential | fk_exists | order_id | order exists | Phase 4 |
| RET-011 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
