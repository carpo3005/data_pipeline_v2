from collections.abc import Mapping
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame, functions as F


@dataclass(frozen=True)
class StandardisationResult:
    """A standardised DataFrame and the invalid-value counts to feed into the audit run."""
    df: DataFrame
    invalid_counts: dict[str, int] = field(default_factory=dict)


def count_invalid(df: DataFrame, checks: Mapping[str, tuple[Column, Column]]) -> dict[str, int]:
    """Count values that had input but became NULL.

    checks maps column name -> (cleaned input expression, output expression), both built on df.
    Returns invalid_<column>_count for each column with failures, plus invalid_value_count in total.
    """
    names = list(checks)
    if not names:
        return {"invalid_value_count": 0}
    aggregates = [
        F.sum(F.when(checks[name][0].isNotNull() & checks[name][1].isNull(), 1).otherwise(0)).alias(f"c{i}")
        for i, name in enumerate(names)
    ]
    row = df.agg(*aggregates).first()
    per_column = {name: int(row[f"c{i}"] or 0) for i, name in enumerate(names)}
    counts = {f"invalid_{name}_count": n for name, n in per_column.items() if n}
    counts["invalid_value_count"] = sum(per_column.values())
    return counts
