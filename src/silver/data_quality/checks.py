from collections.abc import Callable, Mapping

from pyspark.sql import Column, functions as F

from silver.data_quality.raw_copies import raw_column

# A check returns a boolean Column that is True when the row passes.
# A NULL value passes, except for not_null and parsed, so one missing value does not trip several rules.
Check = Callable[[tuple[str, ...], Mapping[str, object]], Column]


def not_null(columns: tuple[str, ...], params: Mapping[str, object]) -> Column:
    return F.col(columns[0]).isNotNull()


def parsed(columns: tuple[str, ...], params: Mapping[str, object]) -> Column:
    """Standardisation could not parse the value: the original was present but the standardised value is NULL."""
    column = columns[0]
    return F.col(raw_column(column)).isNull() | F.col(column).isNotNull()


def matches_regex(columns: tuple[str, ...], params: Mapping[str, object]) -> Column:
    col = F.col(columns[0])
    return col.isNull() | col.rlike(str(params["pattern"]))


def in_set(columns: tuple[str, ...], params: Mapping[str, object]) -> Column:
    col = F.col(columns[0])
    return col.isNull() | col.isin(*params["values"])


def in_range(columns: tuple[str, ...], params: Mapping[str, object]) -> Column:
    col = F.col(columns[0])
    ok = F.lit(True)
    if "minimum" in params:
        ok = ok & (col >= params["minimum"])
    if "maximum" in params:
        ok = ok & (col <= params["maximum"])
    return col.isNull() | ok


def not_future(columns: tuple[str, ...], params: Mapping[str, object]) -> Column:
    col = F.col(columns[0])
    return col.isNull() | (col <= F.current_timestamp())


def column_lte_column(columns: tuple[str, ...], params: Mapping[str, object]) -> Column:
    left, right = F.col(columns[0]), F.col(columns[1])
    return left.isNull() | right.isNull() | (left <= right)


def not_equal_columns(columns: tuple[str, ...], params: Mapping[str, object]) -> Column:
    left, right = F.col(columns[0]), F.col(columns[1])
    return left.isNull() | right.isNull() | (left != right)


def sql_expression(columns: tuple[str, ...], params: Mapping[str, object]) -> Column:
    # SQL NULL (an input was NULL) passes, matching the other checks
    return F.coalesce(F.expr(str(params["expression"])), F.lit(True))


CHECKS: Mapping[str, Check] = {
    "not_null": not_null,
    "parsed": parsed,
    "matches_regex": matches_regex,
    "in_set": in_set,
    "in_range": in_range,
    "not_future": not_future,
    "column_lte_column": column_lte_column,
    "not_equal_columns": not_equal_columns,
    "sql_expression": sql_expression,
}
