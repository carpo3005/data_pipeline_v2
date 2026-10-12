import pytest
from pyspark.sql import functions as F

from pipeline.errors import DataValidationError
from silver.data_quality.evaluate import HARD_COLUMN, SOFT_COLUMN, apply_data_quality, overall_score
from silver.data_quality.rules import HARD, SOFT, Rule

RULES = (
    Rule("T-001", HARD, "not_null", ("id",)),
    Rule("T-002", HARD, "in_range", ("amount",), {"minimum": 0}),
    Rule("T-003", SOFT, "matches_regex", ("code",), {"pattern": r"^C[0-9]$"}),
    Rule("T-004", SOFT, "in_set", ("status",), {"values": ("A", "B")}),
)

# id, amount, code, status
# row_no, id, amount, code, status
ROWS = [
    (1, 1, 5, "C1", "A"),      # clean
    (2, None, 5, "C1", "A"),   # hard T-001
    (3, 3, -1, "X", "Z"),      # hard T-002, soft T-003 and T-004 (quarantined, but soft ids recorded)
    (4, 4, 2, "X", "A"),       # soft T-003 only: trusted with a warning
    (5, 5, None, "C2", "B"),   # clean: NULL amount passes the range check
]
SCHEMA = "row_no int, id int, amount int, code string, status string"


def _by_row(df):
    return {row.row_no: row.asDict() for row in df.collect()}


def test_splits_rows_into_trusted_and_quarantine(isolated_spark):
    outcome = apply_data_quality(isolated_spark.createDataFrame(ROWS, SCHEMA), RULES)
    assert sorted(r.row_no for r in outcome.trusted.select("row_no").collect()) == [1, 4, 5]
    assert sorted(r.row_no for r in outcome.quarantine.select("row_no").collect()) == [2, 3]


def test_records_failed_rule_ids_per_row(isolated_spark):
    outcome = apply_data_quality(isolated_spark.createDataFrame(ROWS, SCHEMA), RULES)
    trusted = _by_row(outcome.trusted.select("row_no", HARD_COLUMN, SOFT_COLUMN))
    quarantine = _by_row(outcome.quarantine.select("row_no", HARD_COLUMN, SOFT_COLUMN))
    assert trusted[4][HARD_COLUMN] == [] and trusted[4][SOFT_COLUMN] == ["T-003"]
    assert trusted[1][SOFT_COLUMN] == []
    assert quarantine[2][HARD_COLUMN] == ["T-001"]
    assert quarantine[3][HARD_COLUMN] == ["T-002"]
    assert quarantine[3][SOFT_COLUMN] == ["T-003", "T-004"]


def test_violation_counts_and_scores_per_rule(isolated_spark):
    metrics = apply_data_quality(isolated_spark.createDataFrame(ROWS, SCHEMA), RULES).metrics
    assert metrics["dq_row_count"] == 5
    assert metrics["dq_quarantine_row_count"] == 2
    assert metrics["dq_trusted_row_count"] == 3
    assert metrics["dq_soft_warning_row_count"] == 1
    assert metrics["dq_clean_row_count"] == 2
    assert metrics["dq_score"] == 40.0
    assert {k: v for k, v in metrics.items() if k.startswith("violations_")} == {
        "violations_T-001": 1,
        "violations_T-002": 1,
        "violations_T-003": 2,
        "violations_T-004": 1,
    }


def test_rule_with_no_violations_is_still_reported_as_zero(isolated_spark):
    df = isolated_spark.createDataFrame([(1, 1, 5, "C1", "A")], SCHEMA)
    metrics = apply_data_quality(df, RULES).metrics
    assert metrics["violations_T-002"] == 0
    assert metrics["dq_score"] == 100.0


def test_output_columns_are_the_input_plus_the_two_id_arrays(isolated_spark):
    df = isolated_spark.createDataFrame(ROWS, SCHEMA)
    outcome = apply_data_quality(df, RULES)
    assert outcome.trusted.columns == df.columns + [HARD_COLUMN, SOFT_COLUMN]


def test_rules_without_soft_checks_still_work(isolated_spark):
    df = isolated_spark.createDataFrame(ROWS, SCHEMA)
    outcome = apply_data_quality(df, [RULES[0]])
    assert outcome.metrics["dq_quarantine_row_count"] == 1
    assert outcome.trusted.count() == 4
    assert outcome.trusted.where(F.size(SOFT_COLUMN) > 0).count() == 0


def test_empty_table_scores_100(isolated_spark):
    df = isolated_spark.createDataFrame([], SCHEMA)
    metrics = apply_data_quality(df, RULES).metrics
    assert metrics["dq_row_count"] == 0
    assert metrics["dq_score"] == 100.0


def test_missing_column_raises_data_validation_error(isolated_spark):
    df = isolated_spark.createDataFrame([(1,)], "id int")
    with pytest.raises(DataValidationError, match="amount"):
        apply_data_quality(df, RULES)


def test_overall_score_weights_entities_by_row_count():
    per_entity = [
        {"dq_row_count": 4, "dq_clean_row_count": 4},
        {"dq_row_count": 6, "dq_clean_row_count": 0},
    ]
    assert overall_score(per_entity) == 40.0
