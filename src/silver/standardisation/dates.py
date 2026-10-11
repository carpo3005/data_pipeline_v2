from pyspark.sql import Column, functions as F


def parse_date(col: Column, formats: tuple[str, ...]) -> Column:
    """First format that parses wins; no match gives NULL."""
    if not formats:
        raise ValueError("formats must not be empty")
    return F.coalesce(*[F.try_to_date(col, fmt) for fmt in formats])


def parse_timestamp(col: Column) -> Column:
    """Parse a timestamp, trimming fractional seconds beyond 6 digits first."""
    return F.try_to_timestamp(F.regexp_replace(col, r"(\.\d{6})\d+$", "$1"))
