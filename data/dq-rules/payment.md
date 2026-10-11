# DQ rules: payment

Source: `silver_stage.payment` → `silver.payment` / `quarantine.payment`. Prefix `PAY`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| PAY-001 | H | completeness | not_null | payment_id | `payment_id` is present |  |
| PAY-002 | H | completeness | not_null | order_id | `order_id` is present |  |
| PAY-003 | H | completeness | not_null | amount | `amount` is present |  |
| PAY-004 | H | validity | in_set | payment_method | payment method is known | CARD, PAYPAL, APPLE_PAY, GIFT_CARD, BANK_TRANSFER |
| PAY-005 | H | validity | in_range | amount | amount is not negative | minimum 0 |
| PAY-006 | S | validity | in_set | payment_status | status is known | CAPTURED, PENDING, FAILED, REFUNDED |
| PAY-007 | S | validity | in_set | currency_code | currency is known | GBP |
| PAY-008 | S | completeness | not_null | transaction_reference | `transaction_reference` is present |  |
| PAY-009 | H | completeness | not_null | payment_timestamp | `payment_timestamp` is present |  |
| PAY-010 | H | plausibility | not_future | payment_timestamp | payment is not in the future |  |
| PAY-011 | S | referential | fk_exists | order_id | order exists | Phase 4 |
| PAY-012 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
