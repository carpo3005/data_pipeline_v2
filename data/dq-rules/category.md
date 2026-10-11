# DQ rules: category

Source: `silver_stage.category` → `silver.category` / `quarantine.category`. Prefix `CAT`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| CAT-001 | H | completeness | not_null | category_id | `category_id` is present |  |
| CAT-002 | H | completeness | not_null | category_name | `category_name` is present |  |
| CAT-003 | S | validity | matches_regex | category_code | code has format CAT-0000 | pattern `^CAT-[0-9]{4}$` |
| CAT-004 | S | validity | is_null | is_active_raw | is_active is a recognised boolean | standardisation could not parse the value |
| CAT-005 | S | validity | matches_regex | parent_category_id | parent id has format CAT0000 (catches `CAT_BAD_*`) | pattern `^CAT[0-9]{4}$`; NULL passes |
| CAT-006 | S | consistency | not_equal_columns | category_id, parent_category_id | category is not its own parent |  |
| CAT-007 | S | referential | fk_exists | parent_category_id | parent exists in `silver_stage.category` | Phase 4 |
| CAT-008 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
