from collections.abc import Mapping
from typing import Literal

from pyspark.sql import Column, functions as F

from silver.standardisation.string_cleaning import trim_whitespace


def map_lookup(col: Column, mapping: Mapping[str, str], case: Literal["upper", "lower"]) -> Column:
    """Replace a value with its mapped value. Keys and input are matched in the given case, trimmed.

    Unknown values, and every value when the mapping is empty, become NULL.
    """
    if not mapping:
        return F.lit(None).cast("string")
    to_case, key_case = (F.upper, str.upper) if case == "upper" else (F.lower, str.lower)
    entries = {key_case(key): value for key, value in mapping.items()}
    lookup_map = F.create_map(*[F.lit(part) for pair in entries.items() for part in pair])
    return F.try_element_at(lookup_map, to_case(trim_whitespace(col)))
