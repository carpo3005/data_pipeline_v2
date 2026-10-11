# DQ rules: customer

Source: `silver_stage.customer` → `silver.customer` / `quarantine.customer`. Prefix `CUS`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| CUS-001 | H | completeness | not_null | customer_id | `customer_id` is present |  |
| CUS-002 | H | completeness | not_null | created_at | `created_at` is present |  |
| CUS-003 | S | validity | matches_regex | email | email looks valid | pattern `^[^@\s]+@[^@\s]+\.[^@\s]+$` |
| CUS-004 | S | validity | is_null | date_of_birth_raw | date of birth parsed | standardisation could not parse the value |
| CUS-005 | H | plausibility | not_future | date_of_birth | date of birth is not in the future |  |
| CUS-006 | S | validity | is_null | phone_raw | phone number normalised to E.164 | standardisation could not parse the value |
| CUS-007 | S | validity | is_null | marketing_opt_in_raw | marketing opt-in is a recognised boolean | standardisation could not parse the value |
| CUS-008 | H | validity | in_set | loyalty_status | loyalty status is known | NONE, BRONZE, SILVER, GOLD |
| CUS-009 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |
| CUS-010 | S | completeness | not_null | email | `email` is present |  |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
