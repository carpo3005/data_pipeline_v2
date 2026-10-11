from collections.abc import Mapping

from pyspark.sql import Column

from silver.standardisation.lookup import map_lookup


def map_uk_subdivision(col: Column, mapping: Mapping[str, str]) -> Column:
    """Map a UK region to its nation, e.g. Greater London -> England. Unknown -> NULL."""
    return map_lookup(col, mapping, "lower")
