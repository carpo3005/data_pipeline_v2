"""Checks the rule code against the rule docs in data/dq-rules, so the two cannot drift apart."""
import re
from pathlib import Path

import pytest

from silver.data_quality.definitions import RULES
from silver.data_quality.rules import PARSED

DOCS = Path(__file__).resolve().parents[3] / "data" / "dq-rules"
DOC_CHECKS = {
    "not_null": "not_null", "is_null": PARSED, "matches_regex": "matches_regex", "in_set": "in_set",
    "in_range": "in_range", "not_future": "not_future", "column_lte_column": "column_lte_column",
    "not_equal_columns": "not_equal_columns", "sql_expression": "sql_expression",
}
ROW = re.compile(r"^\| ([A-Z]{3}-\d{3}) \| ([HS]) \| \w+ \| (\w+) \| ([^|]+) \| [^|]* \| ([^|]*) \|")


def _doc_rules(entity: str):
    """(id, severity, check, columns, notes) for each row of the entity's rule doc, skipping Phase 4 rows."""
    rows = []
    for line in (DOCS / f"{entity}.md").read_text().splitlines():
        match = ROW.match(line)
        if match and "Phase 4" not in match.group(5):
            rule_id, severity, check, columns, notes = match.groups()
            rows.append((rule_id, severity, check, [c.strip() for c in columns.split(",")], notes))
    return rows


def _entities():
    return sorted(path.stem for path in DOCS.glob("*.md") if path.stem != "README")


@pytest.mark.parametrize("entity", _entities())
def test_rule_ids_severity_and_columns_match_docs(entity):
    doc = _doc_rules(entity)
    code = [(r.rule_id, r.severity, r.check, list(r.columns)) for r in RULES.get(entity, ())]
    expected = [
        (rule_id, severity, DOC_CHECKS[check], [c.removesuffix("_raw") for c in columns])
        for rule_id, severity, check, columns, _ in doc
    ]
    assert code == expected


@pytest.mark.parametrize("entity", _entities())
def test_params_match_docs(entity):
    doc = {rule_id: notes for rule_id, _, _, _, notes in _doc_rules(entity)}
    for rule in RULES[entity]:
        notes = doc[rule.rule_id]
        if rule.check == "in_set":
            assert set(rule.params["values"]) == {v.strip() for v in notes.split(",")}, rule.rule_id
        elif rule.check == "in_range":
            assert rule.params == {"minimum": int(re.search(r"minimum (\d+)", notes).group(1))}, rule.rule_id
        elif rule.check in ("matches_regex", "sql_expression"):
            key = "pattern" if rule.check == "matches_regex" else "expression"
            assert rule.params[key] == re.search(r"`([^`]+)`", notes).group(1), rule.rule_id


def test_no_phase4_referential_rules_in_code():
    assert all(rule.check != "fk_exists" for rules in RULES.values() for rule in rules)


def test_rule_ids_are_unique_across_project():
    ids = [rule.rule_id for rules in RULES.values() for rule in rules]
    assert len(ids) == len(set(ids))
