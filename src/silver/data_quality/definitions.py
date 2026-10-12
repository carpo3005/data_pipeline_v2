"""DQ rules per entity. The source of truth is data/dq-rules/<entity>.md; testing/silver/data_quality checks these match.

Phase 4 referential rules (fk_exists) are not included yet; they need the referential helper.
Params match the notes in the rule docs.
"""
from collections.abc import Mapping

from silver.data_quality.rules import HARD, SOFT, Rule

_RETURN_REASONS = ("DEFECTIVE", "UNWANTED", "WRONG_ITEM", "SIZE_FIT", "LATE_DELIVERY", "DAMAGED")
_TIMESTAMPS = ("created_at", "updated_at")

RULES: Mapping[str, tuple[Rule, ...]] = {
    "customer": (
        Rule("CUS-001", HARD, "not_null", ("customer_id",)),
        Rule("CUS-002", HARD, "not_null", ("created_at",)),
        Rule("CUS-003", SOFT, "matches_regex", ("email",), {"pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"}),
        Rule("CUS-004", SOFT, "parsed", ("date_of_birth",)),
        Rule("CUS-005", HARD, "not_future", ("date_of_birth",)),
        Rule("CUS-006", SOFT, "parsed", ("phone",)),
        Rule("CUS-007", SOFT, "parsed", ("marketing_opt_in",)),
        Rule("CUS-008", HARD, "in_set", ("loyalty_status",), {"values": ("NONE", "BRONZE", "SILVER", "GOLD")}),
        Rule("CUS-009", SOFT, "column_lte_column", _TIMESTAMPS),
        Rule("CUS-010", SOFT, "not_null", ("email",)),
    ),
    "address": (
        Rule("ADR-001", HARD, "not_null", ("address_id",)),
        Rule("ADR-002", HARD, "not_null", ("customer_id",)),
        Rule("ADR-003", HARD, "not_null", ("address_line_1",)),
        Rule("ADR-004", HARD, "in_set", ("address_type",), {"values": ("HOME", "SHIPPING", "BILLING")}),
        Rule("ADR-005", SOFT, "matches_regex", ("postal_code",),
             {"pattern": r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$"}),
        Rule("ADR-006", SOFT, "parsed", ("country_code",)),
        Rule("ADR-007", SOFT, "not_null", ("city",)),
        Rule("ADR-008", SOFT, "parsed", ("is_primary",)),
        Rule("ADR-009", SOFT, "not_null", ("uk_nation",)),
        Rule("ADR-010", SOFT, "column_lte_column", _TIMESTAMPS),
    ),
    "category": (
        Rule("CAT-001", HARD, "not_null", ("category_id",)),
        Rule("CAT-002", HARD, "not_null", ("category_name",)),
        Rule("CAT-003", SOFT, "matches_regex", ("category_code",), {"pattern": r"^CAT-[0-9]{4}$"}),
        Rule("CAT-004", SOFT, "parsed", ("is_active",)),
        Rule("CAT-005", SOFT, "matches_regex", ("parent_category_id",), {"pattern": r"^CAT[0-9]{4}$"}),
        Rule("CAT-006", SOFT, "not_equal_columns", ("category_id", "parent_category_id")),
        Rule("CAT-008", SOFT, "column_lte_column", _TIMESTAMPS),
    ),
    "product": (
        Rule("PRD-001", HARD, "not_null", ("product_id",)),
        Rule("PRD-002", HARD, "not_null", ("sku",)),
        Rule("PRD-003", HARD, "not_null", ("unit_price",)),
        Rule("PRD-004", HARD, "in_range", ("unit_price",), {"minimum": 0}),
        Rule("PRD-005", SOFT, "not_null", ("product_name",)),
        Rule("PRD-006", SOFT, "in_set", ("currency_code",), {"values": ("GBP",)}),
        Rule("PRD-007", SOFT, "parsed", ("is_active",)),
        Rule("PRD-008", SOFT, "not_null", ("brand",)),
        Rule("PRD-009", SOFT, "column_lte_column", _TIMESTAMPS),
    ),
    "product_category": (
        Rule("PCT-001", HARD, "not_null", ("product_category_id",)),
        Rule("PCT-002", HARD, "not_null", ("product_id",)),
        Rule("PCT-003", HARD, "not_null", ("category_id",)),
        Rule("PCT-004", SOFT, "parsed", ("is_primary",)),
        Rule("PCT-005", SOFT, "column_lte_column", ("active_from", "active_to")),
        Rule("PCT-006", SOFT, "parsed", ("active_from",)),
        Rule("PCT-007", SOFT, "parsed", ("active_to",)),
        Rule("PCT-010", SOFT, "column_lte_column", _TIMESTAMPS),
    ),
    "order": (
        Rule("ORD-001", HARD, "not_null", ("order_id",)),
        Rule("ORD-002", HARD, "not_null", ("customer_id",)),
        Rule("ORD-003", HARD, "not_null", ("order_timestamp",)),
        Rule("ORD-004", HARD, "in_range", ("order_total_amount",), {"minimum": 0}),
        Rule("ORD-005", SOFT, "in_set", ("order_status",), {"values": ("COMPLETED", "CANCELLED", "PENDING")}),
        Rule("ORD-006", SOFT, "in_set", ("channel",), {"values": ("STORE", "ECOMMERCE", "MOBILE")}),
        Rule("ORD-007", SOFT, "in_set", ("currency_code",), {"values": ("GBP",)}),
        Rule("ORD-008", SOFT, "sql_expression",
             ("subtotal_amount", "discount_amount", "tax_amount", "shipping_amount", "order_total_amount"),
             {"expression": "abs(subtotal_amount - discount_amount + tax_amount + shipping_amount"
                            " - order_total_amount) <= 0.01"}),
        Rule("ORD-009", HARD, "not_future", ("order_timestamp",)),
        Rule("ORD-010", SOFT, "column_lte_column", _TIMESTAMPS),
        Rule("ORD-012", HARD, "not_null", ("order_total_amount",)),
    ),
    "order_item": (
        Rule("OIT-001", HARD, "not_null", ("order_item_id",)),
        Rule("OIT-002", HARD, "not_null", ("quantity",)),
        Rule("OIT-003", HARD, "in_range", ("quantity",), {"minimum": 1}),
        Rule("OIT-004", HARD, "not_null", ("order_id",)),
        Rule("OIT-005", HARD, "not_null", ("product_id",)),
        Rule("OIT-006", HARD, "in_range", ("unit_price",), {"minimum": 0}),
        Rule("OIT-007", SOFT, "sql_expression",
             ("quantity", "unit_price", "discount_amount", "tax_amount", "line_total_amount"),
             {"expression": "abs(quantity * unit_price - discount_amount + tax_amount - line_total_amount)"
                            " <= 0.01"}),
        Rule("OIT-010", SOFT, "in_range", ("discount_amount",), {"minimum": 0}),
        Rule("OIT-011", SOFT, "column_lte_column", _TIMESTAMPS),
    ),
    "payment": (
        Rule("PAY-001", HARD, "not_null", ("payment_id",)),
        Rule("PAY-002", HARD, "not_null", ("order_id",)),
        Rule("PAY-003", HARD, "not_null", ("amount",)),
        Rule("PAY-004", HARD, "in_set", ("payment_method",),
             {"values": ("CARD", "PAYPAL", "APPLE_PAY", "GIFT_CARD", "BANK_TRANSFER")}),
        Rule("PAY-005", HARD, "in_range", ("amount",), {"minimum": 0}),
        Rule("PAY-006", SOFT, "in_set", ("payment_status",),
             {"values": ("CAPTURED", "PENDING", "FAILED", "REFUNDED")}),
        Rule("PAY-007", SOFT, "in_set", ("currency_code",), {"values": ("GBP",)}),
        Rule("PAY-008", SOFT, "not_null", ("transaction_reference",)),
        Rule("PAY-009", HARD, "not_null", ("payment_timestamp",)),
        Rule("PAY-010", HARD, "not_future", ("payment_timestamp",)),
        Rule("PAY-012", SOFT, "column_lte_column", _TIMESTAMPS),
    ),
    "return": (
        Rule("RET-001", HARD, "not_null", ("return_id",)),
        Rule("RET-002", HARD, "not_null", ("order_id",)),
        Rule("RET-003", HARD, "not_null", ("customer_id",)),
        Rule("RET-004", HARD, "not_null", ("return_timestamp",)),
        Rule("RET-005", HARD, "not_null", ("total_refund_amount",)),
        Rule("RET-006", HARD, "in_range", ("total_refund_amount",), {"minimum": 0}),
        Rule("RET-007", SOFT, "in_set", ("return_status",), {"values": ("COMPLETED", "PENDING", "REJECTED")}),
        Rule("RET-008", SOFT, "in_set", ("return_reason",), {"values": _RETURN_REASONS}),
        Rule("RET-009", HARD, "not_future", ("return_timestamp",)),
        Rule("RET-011", SOFT, "column_lte_column", _TIMESTAMPS),
    ),
    "return_item": (
        Rule("RIT-001", HARD, "not_null", ("return_item_id",)),
        Rule("RIT-002", HARD, "not_null", ("return_id",)),
        Rule("RIT-003", HARD, "not_null", ("order_item_id",)),
        Rule("RIT-004", HARD, "not_null", ("quantity",)),
        Rule("RIT-005", HARD, "in_range", ("quantity",), {"minimum": 1}),
        Rule("RIT-006", HARD, "not_null", ("refund_amount",)),
        Rule("RIT-007", HARD, "in_range", ("refund_amount",), {"minimum": 0}),
        Rule("RIT-008", SOFT, "in_set", ("return_reason",), {"values": _RETURN_REASONS}),
        Rule("RIT-009", SOFT, "not_null", ("product_id",)),
        Rule("RIT-012", SOFT, "column_lte_column", _TIMESTAMPS),
    ),
}


def rules_for(entity: str) -> tuple[Rule, ...]:
    if entity not in RULES:
        raise KeyError(f"No DQ rules for entity '{entity}'; known: {sorted(RULES)}")
    return RULES[entity]
