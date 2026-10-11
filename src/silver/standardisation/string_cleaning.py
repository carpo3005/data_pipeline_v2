from typing import Literal
from pyspark.sql import Column, functions as F

TextCase = Literal["upper", "lower", "title"]


def trim_whitespace(col: Column) -> Column:
    """Trim both ends and collapse internal whitespace runs to one space."""
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def nullify_tokens(col: Column, tokens: tuple[str, ...]) -> Column:
    """NULL when the value equals any token (case-insensitive). Expects trimmed input."""
    if not tokens:
        return col
    lowered_tokens = [token.lower() for token in tokens]
    return F.when(F.lower(col).isin(lowered_tokens), F.lit(None)).otherwise(col)


def apply_case(col: Column, case: TextCase | None) -> Column:
    """Apply upper, lower or title case; None leaves the column unchanged."""
    if case is None:
        return col
    if case == "upper":
        return F.upper(col)
    if case == "lower":
        return F.lower(col)
    if case == "title":
        return F.initcap(col)
    raise ValueError(f"Unknown case: {case!r}")


def clean_string(col: Column, tokens: tuple[str, ...], case: TextCase | None = None) -> Column:
    """Trim, turn null tokens into NULL, then apply case."""
    return apply_case(nullify_tokens(trim_whitespace(col), tokens), case)
