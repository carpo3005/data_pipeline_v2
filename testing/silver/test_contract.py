import pytest

from silver.contract import CastRule, EntityContract

COLUMN_RULES = {"email": "lower", "is_active": "boolean"}


def _contract(contract, column_rules=COLUMN_RULES, valid_rules=None):
    return EntityContract.from_dict("customer", contract, column_rules, "src.customer", "tgt.customer", valid_rules)


def test_string_business_key_becomes_tuple():
    assert _contract({"business_key": "address_id"}).business_key == ("address_id",)


def test_list_business_key_is_kept_as_tuple():
    assert _contract({"business_key": ["order_id", "line"]}).business_key == ("order_id", "line")


def test_cast_rules_parsed_from_dict():
    contract = _contract({"business_key": ["id"], "cast_rules": {"amount": "DECIMAL(18,2)"}})
    assert contract.cast_rules == (CastRule("amount", "DECIMAL(18,2)"),)


def test_from_json_reads_same_shape():
    text = '{"business_key": ["id"], "cast_rules": {"quantity": "INT"}}'
    contract = EntityContract.from_json("order_item", text, {}, "src.oi", "tgt.oi")
    assert contract.business_key == ("id",)
    assert contract.cast_rules == (CastRule("quantity", "INT"),)


def test_missing_business_key_raises():
    with pytest.raises(ValueError, match="missing 'business_key'"):
        _contract({"cast_rules": {}})


def test_unknown_key_raises():
    with pytest.raises(ValueError, match="unknown contract keys"):
        _contract({"business_key": ["id"], "cast_rule": {}})


def test_empty_business_key_raises():
    with pytest.raises(ValueError, match="business_key is empty"):
        _contract({"business_key": []})


def test_column_in_cast_and_column_rules_raises():
    with pytest.raises(ValueError, match="both cast_rules and column_rules"):
        _contract({"business_key": ["id"], "cast_rules": {"email": "STRING"}})


def test_unregistered_rule_raises_when_valid_rules_given():
    with pytest.raises(ValueError, match="not registered: \\['phone'\\]"):
        _contract({"business_key": ["id"]}, {"mobile": "phone"}, valid_rules={"lower", "boolean"})


def test_registered_rules_pass_validation():
    contract = _contract({"business_key": ["id"]}, COLUMN_RULES, valid_rules={"lower", "boolean"})
    assert contract.column_rules == COLUMN_RULES
