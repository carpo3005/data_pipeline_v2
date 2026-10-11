# DQ rules: address

Source: `silver_stage.address` → `silver.address` / `quarantine.address`. Prefix `ADR`.

| Rule ID | Sev | Category | Check | Columns | Description | Params / notes |
|---|---|---|---|---|---|---|
| ADR-001 | H | completeness | not_null | address_id | `address_id` is present |  |
| ADR-002 | H | completeness | not_null | customer_id | `customer_id` is present |  |
| ADR-003 | H | completeness | not_null | address_line_1 | `address_line_1` is present |  |
| ADR-004 | H | validity | in_set | address_type | address type is known | HOME, SHIPPING, BILLING |
| ADR-005 | S | validity | matches_regex | postal_code | postcode has UK format | pattern `^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$` |
| ADR-006 | S | validity | is_null | country_code_raw | country mapped to ISO code | standardisation could not parse the value |
| ADR-007 | S | completeness | not_null | city | `city` is present |  |
| ADR-008 | S | validity | is_null | is_primary_raw | is_primary is a recognised boolean | standardisation could not parse the value |
| ADR-009 | S | completeness | not_null | uk_nation | UK nation derived from `state_region` |  |
| ADR-010 | S | consistency | column_lte_column | created_at, updated_at | `updated_at` is not before `created_at` |  |
| ADR-011 | S | referential | fk_exists | customer_id | customer exists in `silver_stage.customer` | Phase 4 |

Tests per rule (generated from the check unit tests, applied to this entity): one failing row, one passing row, and for `not_null`/`is_null` rules a NULL case; the integration suite covers the real bad rows listed in `../07-testing-strategy.md`.
