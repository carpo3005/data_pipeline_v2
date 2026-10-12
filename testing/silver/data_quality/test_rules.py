import pytest

from silver.data_quality.rules import HARD, SOFT, Rule, raw_columns


def test_rejects_unknown_severity():
    with pytest.raises(ValueError, match="severity"):
        Rule("X-001", "X", "not_null", ("a",))


def test_rejects_unknown_check():
    with pytest.raises(ValueError, match="unknown check"):
        Rule("X-001", HARD, "no_such_check", ("a",))


def test_rejects_rule_without_columns():
    with pytest.raises(ValueError, match="no columns"):
        Rule("X-001", SOFT, "not_null", ())


def test_raw_columns_lists_only_parsed_rules_once():
    rules = (
        Rule("X-001", SOFT, "parsed", ("phone",)),
        Rule("X-002", HARD, "not_null", ("phone",)),
        Rule("X-003", SOFT, "parsed", ("phone",)),
        Rule("X-004", SOFT, "parsed", ("is_primary",)),
    )
    assert raw_columns(rules) == ("phone", "is_primary")
