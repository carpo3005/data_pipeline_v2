from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, functions as F

from pipeline.validation import assert_columns_exist
from silver.data_quality.raw_copies import raw_column
from silver.data_quality.rules import PARSED, Rule

HARD_COLUMN = "_dq_hard_failed_rule_ids"
SOFT_COLUMN = "_dq_soft_failed_rule_ids"
ID_TYPE = "array<string>"


@dataclass(frozen=True)
class DqOutcome:
    """Trusted rows (no hard failure, soft warnings kept), quarantined rows (any hard failure), and audit metrics."""
    trusted: DataFrame
    quarantine: DataFrame
    metrics: dict[str, int | float]


def apply_data_quality(df: DataFrame, rules: Sequence[Rule]) -> DqOutcome:
    _check_columns(df, rules)
    hard = [rule for rule in rules if rule.is_hard]
    soft = [rule for rule in rules if not rule.is_hard]
    flagged = df.withColumns({
        HARD_COLUMN: _failed_ids(hard),
        SOFT_COLUMN: _failed_ids(soft),
    })
    metrics = _metrics(flagged, rules)
    return DqOutcome(
        trusted=flagged.where(F.size(HARD_COLUMN) == 0),
        quarantine=flagged.where(F.size(HARD_COLUMN) > 0),
        metrics=metrics,
    )


def overall_score(metrics_per_entity: Iterable[Mapping[str, int | float]]) -> float:
    """Percentage of rows that passed every rule, across several entities."""
    total = clean = 0
    for metrics in metrics_per_entity:
        total += int(metrics["dq_row_count"])
        clean += int(metrics["dq_clean_row_count"])
    return _score(clean, total)


def _check_columns(df: DataFrame, rules: Sequence[Rule]) -> None:
    needed = {column for rule in rules for column in rule.columns}
    needed |= {raw_column(column) for rule in rules if rule.check == PARSED for column in rule.columns}
    assert_columns_exist(df, sorted(needed))


def _failed_ids(rules: Sequence[Rule]) -> Column:
    """Array of the IDs of the rules this row fails (empty array when none)."""
    if not rules:
        return F.array().cast(ID_TYPE)
    return F.array_compact(F.array(*[F.when(~rule.passes(), F.lit(rule.rule_id)) for rule in rules]))


def _metrics(flagged: DataFrame, rules: Sequence[Rule]) -> dict[str, int | float]:
    """One aggregation over the flagged rows: totals, violations per rule, and row outcomes."""
    hard_count = F.size(HARD_COLUMN)
    soft_count = F.size(SOFT_COLUMN)
    counts = {
        "dq_row_count": F.count(F.lit(1)),
        "dq_quarantine_row_count": _count_where(hard_count > 0),
        "dq_soft_warning_row_count": _count_where((hard_count == 0) & (soft_count > 0)),
        "dq_clean_row_count": _count_where((hard_count == 0) & (soft_count == 0)),
    }
    for rule in rules:
        counts[f"violations_{rule.rule_id}"] = _count_where(~rule.passes())

    row = flagged.agg(*[expr.alias(name) for name, expr in counts.items()]).first()
    metrics: dict[str, int | float] = {name: int(row[name] or 0) for name in counts}
    metrics["dq_trusted_row_count"] = metrics["dq_row_count"] - metrics["dq_quarantine_row_count"]
    metrics["dq_score"] = _score(metrics["dq_clean_row_count"], metrics["dq_row_count"])
    return metrics


def _count_where(condition: Column) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0))


def _score(clean: int, total: int) -> float:
    # An empty table has nothing failing, so it scores 100
    return round(100.0 * clean / total, 2) if total else 100.0
