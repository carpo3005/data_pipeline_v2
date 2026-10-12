from collections.abc import Sequence

from pyspark.sql import Column, functions as F

UNKNOWN_KEY = -1                       # surrogate key of the seeded "unknown" member row in each dimension
END_OF_TIME = "9999-12-31 00:00:00"    # open end of a current SCD2 version


def _col(value: str | Column) -> Column:
    return F.col(value) if isinstance(value, str) else value


def surrogate_key(*columns: str | Column) -> Column:
    """Deterministic 64-bit key (xxhash64). The same inputs always give the same key, so reruns keep keys stable."""
    return F.xxhash64(*[_col(c) for c in columns])


def date_key(column: str | Column) -> Column:
    """YYYYMMDD as an INT: the business key of dim_date."""
    c = _col(column)
    return (F.year(c) * 10000 + F.month(c) * 100 + F.dayofmonth(c)).cast("int")


def attribute_hash(columns: Sequence[str]) -> Column:
    """SHA-256 of the attribute columns, for change detection. NULLs are included in the hash."""
    row_json = F.to_json(F.struct(*[F.col(c) for c in columns]), options={"ignoreNullFields": "false"})
    return F.sha2(row_json, 256)
