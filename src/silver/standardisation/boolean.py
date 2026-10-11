
from silver.standardisation.string_cleaning import trim_whitespace
from pyspark.sql import Column, functions as F


def assert_disjoint(true_values: tuple[str, ...], false_values: tuple[str, ...]) -> None:
    overlap = {v.upper() for v in true_values} & {v.upper() for v in false_values}
    if overlap:
        raise ValueError(f"true_values and false_values overlap: {sorted(overlap)}")


def normalise_boolean(col: Column, true_values: tuple[str, ...], false_values: tuple[str, ...]) -> Column:
    """True/False for the listed values (case-insensitive), NULL for anything else."""
    assert_disjoint(true_values, false_values)
    value = F.upper(trim_whitespace(col))
    return (
        F.when(value.isin([v.upper() for v in true_values]), F.lit(True))
        .when(value.isin([v.upper() for v in false_values]), F.lit(False))
    )