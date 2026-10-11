# DQ rules by entity

Rule IDs are `<PREFIX>-<NNN>`, unique project-wide. **H** = hard (row goes to quarantine), **S** = soft (row stays in trusted with the ID in `_dq_soft_failed_rule_ids`). Each entity's code lives in `Configs/dq_rules/<entity>.py` and must list exactly these IDs (`test_config_catalogs.py` enforces it). Allowed values and thresholds shown here are the `params` in code and are the only place to change them.

Rules marked *Phase 4* need the referential helper; implement after all single-table rules pass.

| Entity | Prefix | Rules | Hard | Soft |
|---|---|---|---|---|
| [customer](customer.md) | CUS | 10 | 4 | 6 |
| [address](address.md) | ADR | 11 | 4 | 7 |
| [category](category.md) | CAT | 8 | 2 | 6 |
| [product](product.md) | PRD | 9 | 4 | 5 |
| [product_category](product_category.md) | PCT | 10 | 3 | 7 |
| [order](order.md) | ORD | 12 | 6 | 6 |
| [order_item](order_item.md) | OIT | 11 | 6 | 5 |
| [payment](payment.md) | PAY | 12 | 7 | 5 |
| [return](return.md) | RET | 11 | 7 | 4 |
| [return_item](return_item.md) | RIT | 12 | 7 | 5 |
