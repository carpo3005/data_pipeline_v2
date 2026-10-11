# DQ rules: product_category

Source: `silver_stage.product_category` → `silver.product_category` / `quarantine.product_category`. Prefix `PCT`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| PCT-001 | H | completeness | not_null | product_category_id | `product_category_id` is present |  |
| PCT-002 | H | completeness | not_null | product_id | `product_id` is present |  |
| PCT-003 | H | completeness | not_null | category_id | `category_id` is present |  |
| PCT-004 | S | validity | is_null | is_primary_raw | is_primary is a recognised boolean | standardisation could not parse the value |
| PCT-005 | S | consistency | column_lte_column | active_from, active_to | `active_from` is not after `active_to` |  |
| PCT-006 | S | validity | is_null | active_from_raw | active_from parsed | standardisation could not parse the value |
| PCT-007 | S | validity | is_null | active_to_raw | active_to parsed | standardisation could not parse the value |
| PCT-008 | S | referential | fk_exists | product_id | product exists | Phase 4 |
| PCT-009 | S | referential | fk_exists | category_id | category exists | Phase 4 |
| PCT-010 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
